<#
Elimina lo que cobra por hora (job de Dataflow, Memorystore y VM) y verifica que no quede nada.

  .\infra\apagar_y_verificar.ps1      (desde la raíz del repositorio)

No toca Cloud Storage, Pub/Sub ni BigQuery: los resultados y las suscripciones se conservan.
El job cancelado sigue visible en la consola de Dataflow con su Job Graph y sus métricas.
#>
$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "config.ps1")

Paso "1. Cancelando jobs de Dataflow activos"
$jobs = GS dataflow jobs list --region=$REGION --status=active --filter="name=$JOB" --format="value(id)"
foreach ($id in @($jobs | Where-Object { $_ })) {
    G dataflow jobs cancel $id --region=$REGION | Out-Null
    Write-Host "Cancelación pedida: $id"
}
if (-not $jobs) { Write-Host "No hay jobs activos." }

Paso "2. Eliminando Memorystore"
$r = GS redis instances list --region=$REGION --format="value(name.basename())"
if ($r -contains $REDIS) {
    G redis instances delete $REDIS --region=$REGION | Out-Null
    Write-Host "Eliminada: $REDIS"
} else { Write-Host "No existe." }

Paso "3. Eliminando la VM"
$v = GS compute instances list --filter="name=$VM" --format="value(name)"
if ($v) {
    G compute instances delete $VM --zone=$ZONA --delete-disks=all | Out-Null
    Write-Host "Eliminada: $VM"
} else { Write-Host "No existe." }

Paso "4. Verificación"
$limite = (Get-Date).AddMinutes(10)
do {
    $activos = GS dataflow jobs list --region=$REGION --status=active --format="value(id)"
    if (-not $activos) { break }
    Write-Host "  $(Get-Date -Format HH:mm:ss)  esperando que terminen los workers..."
    Start-Sleep -Seconds 20
} while ((Get-Date) -lt $limite)

$pendientes = @()
if (GS dataflow jobs list --region=$REGION --status=active --format="value(id)") { $pendientes += "Dataflow" }
if (GS redis instances list --region=$REGION --format="value(name)") { $pendientes += "Memorystore" }
if (GS compute instances list --format="value(name)") { $pendientes += "Compute Engine" }
if (GS compute disks list --format="value(name)") { $pendientes += "discos" }

if (Test-Path $ESTADO) {
    $e = Get-Content $ESTADO -Raw | ConvertFrom-Json
    $horas = ((Get-Date).ToUniversalTime() - [datetime]::Parse($e.inicio_utc).ToUniversalTime()).TotalHours
    Write-Host ("Tiempo encendido: {0:N2} h. Costo aproximado: USD {1:N2} (confirmar en Billing en 24 a 48 h)." -f $horas, ($horas * $USD_HORA_APROX))
}

if ($pendientes.Count -eq 0) {
    Write-Host "OK: no queda nada que cobre por hora." -ForegroundColor Green
} else {
    Write-Host "ATENCIÓN: siguen activos: $($pendientes -join ', '). Vuelve a ejecutar este script o revisa la consola." -ForegroundColor Red
    exit 1
}
