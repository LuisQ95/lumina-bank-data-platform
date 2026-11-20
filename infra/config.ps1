<#
.SYNOPSIS
    Configuración compartida por los scripts de infra/.

.DESCRIPTION
    Carga .env de la raíz sin pisar variables ya definidas y fija los nombres, la región y los tamaños
    de los recursos. Cambiar un valor aquí lo cambia en el despliegue y en el apagado, así que cada
    recreación sale igual.

    Recursos que cobran por hora y se crean por sesión:
      Memorystore for Redis   Basic, 1 GB, Redis 7.2, red default con peering directo.
      VM de verificación      e2-micro con redis-cli (capa gratuita en us-central1).
      Dataflow                1 worker n1-standard-2 con Streaming Engine.

    Recurso permanente sin costo fijo:
      Cloud Run (API)         Escala a cero, máximo 1 instancia, cuenta de servicio propia.

    Funciones:
      G     Ejecuta gcloud en el proyecto sin preguntas y lanza un error si falla.
      GS    Igual que G, pero devuelve $null si falla (para consultas de estado).
      Paso  Imprime el título de un paso.
#>

$RAIZ = Split-Path $PSScriptRoot

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

$REDIS         = "lumina-redis"
$REDIS_VERSION = "redis_7_2"
$REDIS_GB      = 1

$VM = "lumina-redis-cli"

$JOB            = "lumina-stream-redis"
$MAQUINA_WORKER = "n1-standard-2"

$SERVICIO_API = "lumina-api"
$CUENTA_API   = "lumina-api"

$BUCKET    = Valor "LUMINA_BUCKET_BRONZE" "lumina-bank-bronze-$PROYECTO"
$PERFILES  = "gs://$BUCKET/dataflow/perfiles_muestra.jsonl"
$SUBS      = @("transacciones-sub-dataflow", "geo-sub-dataflow", "atm-sub-dataflow", "clicks-sub-dataflow")
$ETIQUETAS = "proyecto=lumina,entorno=demo"

$USD_HORA_APROX = 0.31

$PY = Valor "LUMINA_PYTHON" ""
if (-not $PY) {
    $venv = Join-Path $RAIZ ".venv\Scripts\python.exe"
    $PY = if (Test-Path $venv) { $venv } else { (Get-Command python -ErrorAction SilentlyContinue).Source }
}
$env:PYTHONPATH = Join-Path $RAIZ "src"

$GCLOUD = Valor "GCLOUD_BIN" ""
if (-not $GCLOUD) {
    $cmd = Get-Command gcloud -ErrorAction SilentlyContinue
    $GCLOUD = if ($cmd) { $cmd.Source } else { Join-Path $env:LOCALAPPDATA "Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" }
}

$REPORTES = Valor "LUMINA_REPORTS_DIR" (Join-Path $RAIZ "reports")
New-Item -ItemType Directory -Force $REPORTES | Out-Null
$ESTADO = Join-Path $REPORTES "estado_despliegue_dataflow.json"

function G {
    $salida = & $GCLOUD @args "--project=$PROYECTO" --quiet
    if ($LASTEXITCODE -ne 0) { throw "gcloud falló ($LASTEXITCODE): $($args -join ' ')" }
    return $salida
}

function GS {
    $salida = & $GCLOUD @args "--project=$PROYECTO" --quiet 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return $salida
}

function Paso($texto) { Write-Host ""; Write-Host "=== $texto ===" -ForegroundColor Cyan }
