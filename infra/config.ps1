# Configuración compartida por desplegar_dataflow_memorystore.ps1 y apagar_y_verificar.ps1.
# Los valores se toman de variables de entorno (o de .env en la raíz); si no existen, se usan los de abajo.
# Cambiar un valor aquí lo cambia en el despliegue y en el apagado, así cada recreación sale igual.

$RAIZ = Split-Path $PSScriptRoot

# Carga .env de la raíz sin pisar variables ya definidas
$envFile = Join-Path $RAIZ ".env"
if (Test-Path $envFile) {
    foreach ($linea in Get-Content $envFile) {
        if ($linea -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$' -and -not (Test-Path "env:$($Matches[1])")) {
            Set-Item "env:$($Matches[1])" ($Matches[2].Trim('"').Trim("'"))
        }
    }
}

function Valor($nombre, $porDefecto) { $v = [Environment]::GetEnvironmentVariable($nombre); if ($v) { $v } else { $porDefecto } }

$PROYECTO = Valor "GCP_PROJECT" "lumina-bank-utec-2026"
$REGION   = Valor "GCP_REGION" "us-central1"
$ZONA     = Valor "GCP_ZONE" "$REGION-a"

# Memorystore for Redis: Basic, 1 GB, Redis 7.2, red default (peering directo)
$REDIS         = "lumina-redis"
$REDIS_VERSION = "redis_7_2"
$REDIS_GB      = 1

# VM para demostrar la conexión con redis-cli (e2-micro, capa gratuita en us-central1)
$VM = "lumina-redis-cli"

# Dataflow: 1 worker fijo, Streaming Engine
$JOB            = "lumina-stream-redis"
$MAQUINA_WORKER = "n1-standard-2"

$BUCKET    = Valor "LUMINA_BUCKET_BRONZE" "lumina-bank-bronze-$PROYECTO"
$PERFILES  = "gs://$BUCKET/dataflow/perfiles_muestra.jsonl"
$SUBS      = @("transacciones-sub-dataflow", "geo-sub-dataflow", "atm-sub-dataflow", "clicks-sub-dataflow")
$ETIQUETAS = "proyecto=lumina,entorno=demo"

# Costo aproximado por hora con todo encendido (precios de lista; se confirma en Cloud Billing)
$USD_HORA_APROX = 0.31

# Python con Apache Beam: LUMINA_PYTHON, el .venv del repositorio o el python del PATH
$PY = Valor "LUMINA_PYTHON" ""
if (-not $PY) {
    $venv = Join-Path $RAIZ ".venv\Scripts\python.exe"
    $PY = if (Test-Path $venv) { $venv } else { (Get-Command python -ErrorAction SilentlyContinue).Source }
}
$env:PYTHONPATH = Join-Path $RAIZ "src"

# CLI de Google Cloud: GCLOUD_BIN, el PATH o la instalación por defecto de Windows
$GCLOUD = Valor "GCLOUD_BIN" ""
if (-not $GCLOUD) {
    $cmd = Get-Command gcloud -ErrorAction SilentlyContinue
    $GCLOUD = if ($cmd) { $cmd.Source } else { Join-Path $env:LOCALAPPDATA "Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" }
}

$REPORTES = Valor "LUMINA_REPORTS_DIR" (Join-Path $RAIZ "reports")
New-Item -ItemType Directory -Force $REPORTES | Out-Null
$ESTADO = Join-Path $REPORTES "estado_despliegue_dataflow.json"

function G {
    # Ejecuta gcloud en el proyecto, sin preguntas. Devuelve la salida; lanza error si falla.
    $salida = & $GCLOUD @args "--project=$PROYECTO" --quiet
    if ($LASTEXITCODE -ne 0) { throw "gcloud falló ($LASTEXITCODE): $($args -join ' ')" }
    return $salida
}

function GS {
    # Igual que G, pero para consultas: si falla (por ejemplo, API aún no habilitada) devuelve $null.
    $salida = & $GCLOUD @args "--project=$PROYECTO" --quiet 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return $salida
}

function Paso($texto) { Write-Host ""; Write-Host "=== $texto ===" -ForegroundColor Cyan }
