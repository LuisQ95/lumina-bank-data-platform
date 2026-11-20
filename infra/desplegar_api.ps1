<#
.SYNOPSIS
    Despliega la API y la página web de Lumina Bank en Cloud Run.

.DESCRIPTION
    Construye la imagen con el Dockerfile de la raíz (gcloud run deploy --source) y la publica como un
    servicio que escala a cero, con máximo una instancia. El servicio usa una cuenta de servicio propia
    con permisos mínimos: leer BigQuery, publicar en Pub/Sub y leer los perfiles del bucket Bronze.

    Sin parámetros solo verifica: cuenta, pruebas de la API y estado del servicio. No crea nada.

.PARAMETER Desplegar
    Habilita las APIs, crea la cuenta de servicio con sus roles y despliega el servicio.

.PARAMETER ConectarMemorystore
    Agrega salida directa a la VPC y REDIS_HOST para que la API lea el estado en Memorystore. Requiere
    que infra/desplegar_dataflow_memorystore.ps1 -AceptoCosto ya haya creado la instancia.

.EXAMPLE
    .\infra\desplegar_api.ps1
    .\infra\desplegar_api.ps1 -Desplegar
    .\infra\desplegar_api.ps1 -Desplegar -ConectarMemorystore
#>
param([switch]$Desplegar, [switch]$ConectarMemorystore)

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "config.ps1")
Set-Location $RAIZ
$env:PYTHONNOUSERSITE = "1"
$cuentaApi = "$CUENTA_API@$PROYECTO.iam.gserviceaccount.com"

Paso "1. Cuenta y proyecto"
$cuenta = & $GCLOUD config get-value account --quiet
Write-Host "Cuenta: $cuenta   Proyecto: $PROYECTO   Región: $REGION   Servicio: $SERVICIO_API"

Paso "2. Pruebas de la API"
& $PY -m pytest "tests\test_api.py"
if ($LASTEXITCODE -ne 0) { throw "Fallan las pruebas de la API. No se despliega." }

Paso "3. Estado actual del servicio"
$url = GS run services describe $SERVICIO_API --region=$REGION --format="value(status.url)"
Write-Host "Servicio: $(if ($url) { $url } else { 'no desplegado' })"

if (-not $Desplegar) {
    Write-Host ""
    Write-Host "Verificación terminada. No se creó nada." -ForegroundColor Green
    Write-Host "Para desplegar:  .\infra\desplegar_api.ps1 -Desplegar"
    exit 0
}

Paso "4. APIs de Cloud Run, Cloud Build y Artifact Registry"
G services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com | Out-Null

Paso "5. Cuenta de servicio con permisos mínimos"
if (-not (GS iam service-accounts describe $cuentaApi --format="value(email)")) {
    G iam service-accounts create $CUENTA_API --display-name="Lumina API (Cloud Run)" | Out-Null
    Start-Sleep -Seconds 10
}
foreach ($rol in @("roles/bigquery.jobUser", "roles/bigquery.dataViewer", "roles/pubsub.publisher")) {
    G projects add-iam-policy-binding $PROYECTO --member="serviceAccount:$cuentaApi" --role=$rol --condition=None | Out-Null
    Write-Host "  $rol"
}
& $GCLOUD storage buckets add-iam-policy-binding "gs://$BUCKET" --member="serviceAccount:$cuentaApi" --role="roles/storage.objectViewer" --project=$PROYECTO --quiet | Out-Null
Write-Host "  roles/storage.objectViewer en gs://$BUCKET"

Paso "6. Construcción y despliegue en Cloud Run (3 a 6 minutos)"
$variables = "GCP_PROJECT=$PROYECTO,GCP_REGION=$REGION"
$red = @()
if ($ConectarMemorystore) {
    if (-not (Test-Path $RUTA_ESTADO)) { throw "No existe $RUTA_ESTADO. Despliega primero Dataflow y Memorystore." }
    $estado = Get-Content $RUTA_ESTADO -Raw | ConvertFrom-Json
    $variables += ",REDIS_HOST=$($estado.redis_host),REDIS_PORT=$($estado.redis_port)"
    $red = @("--network=default", "--subnet=default", "--vpc-egress=private-ranges-only")
    Write-Host "Con salida a la VPC hacia Memorystore $($estado.redis_host)"
} else {
    $red = @("--clear-network")
}
G run deploy $SERVICIO_API --source $RAIZ --region=$REGION --service-account=$cuentaApi `
    --allow-unauthenticated --port=8080 --memory=512Mi --cpu=1 --min-instances=0 --max-instances=1 `
    --set-env-vars=$variables --labels=$ETIQUETAS @red | Out-Host

$url = G run services describe $SERVICIO_API --region=$REGION --format="value(status.url)"
Paso "7. Verificación"
$salud = Invoke-RestMethod "$url/api/salud" -TimeoutSec 60
Write-Host ($salud | ConvertTo-Json -Compress)
Write-Host ""
Write-Host "API desplegada: $url" -ForegroundColor Green
Write-Host "Documentación de endpoints: README, sección 4.4"
