<#
.SYNOPSIS
    Crea Memorystore y la VM con redis-cli, y lanza el job de Dataflow.

.DESCRIPTION
    Sin parámetros solo verifica: credenciales, pruebas del pipeline, armado del job (dry run) y
    recursos existentes. No crea nada ni cobra. Ejecutar desde la raíz del repositorio.

    Después del despliegue:
      .\infra\desplegar_api.ps1 -Desplegar -ConectarMemorystore            Conecta la API a Redis (opcional).
      python -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50   Publica tráfico de carga.
      python -m lumina.ops.reporte_dataflow                                Junta la evidencia antes de apagar.
      .\infra\apagar_y_verificar.ps1                                       Elimina todo lo que cobra.

.PARAMETER AceptoCosto
    Despliega. Cobra por hora hasta ejecutar apagar_y_verificar.ps1.

.PARAMETER SinVM
    No crea la VM de redis-cli.
#>
param([switch]$AceptoCosto, [switch]$SinVM)

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "config.ps1")
Set-Location $RAIZ
$env:PYTHONNOUSERSITE = "1"

Paso "1. Cuenta y proyecto"
$cuenta = & $GCLOUD config get-value account --quiet
Write-Host "Cuenta: $cuenta   Proyecto: $PROYECTO   Región: $REGION"

Paso "2. Entorno Python con Apache Beam"
if (-not $PY -or -not (Test-Path $PY)) { throw "No hay Python. Crea el entorno: python -m venv .venv; .venv\Scripts\pip install -e .[dataflow,dev]" }
& $PY -c "import apache_beam, redis, sys; print('Python', sys.version.split()[0], '| Beam', apache_beam.__version__, '| redis', redis.__version__)"
if ($LASTEXITCODE -ne 0) { throw "El entorno Python no está listo." }

Paso "3. Pruebas locales del pipeline"
& $PY -m pytest "tests\test_dataflow_pipeline.py"
if ($LASTEXITCODE -ne 0) { throw "Fallan las pruebas locales. No se despliega." }

Paso "4. Armado del job de Dataflow sin enviarlo (dry run)"
& $PY "src\lumina\streaming\dataflow_pipeline.py" --runner DataflowRunner --project $PROYECTO --region $REGION `
    --temp_location "gs://$BUCKET/dataflow/temp" --redis_host 10.0.0.1 --perfiles_gcs $PERFILES --dry_run=True | Out-Null
if ($LASTEXITCODE -ne 0) { throw "El job no se pudo armar." }
Write-Host "El grafo se arma correctamente para DataflowRunner."

Paso "5. Recursos de pago existentes"
$redisActual = GS redis instances list --region=$REGION --format="value(name.basename())"
$vmActual    = GS compute instances list --filter="name=$VM" --format="value(name)"
$jobsActivos = GS dataflow jobs list --region=$REGION --status=active --filter="name=$JOB" --format="value(id)"
Write-Host "Memorystore: $(if ($redisActual) { $redisActual } else { 'ninguna' })"
Write-Host "VM:          $(if ($vmActual) { $vmActual } else { 'ninguna' })"
Write-Host "Dataflow:    $(if ($jobsActivos) { $jobsActivos } else { 'sin jobs activos' })"

if (-not $AceptoCosto) {
    Write-Host ""
    Write-Host "Verificación terminada. No se creó nada." -ForegroundColor Green
    Write-Host "Para desplegar (cobra ~USD $USD_HORA_APROX/h hasta apagar):  .\infra\desplegar_dataflow_memorystore.ps1 -AceptoCosto"
    exit 0
}

$inicio = (Get-Date).ToUniversalTime()
$estado = [ordered]@{ inicio_utc = $inicio.ToString("o"); redis = $REDIS; vm = $(if ($SinVM) { $null } else { $VM }); job_nombre = $JOB }
function Guardar { ($estado | ConvertTo-Json) | Set-Content -Path $RUTA_ESTADO -Encoding UTF8 }

try {
    Paso "6. API de Memorystore"
    G services enable redis.googleapis.com | Out-Null

    Paso "7. Instancia de Memorystore (5 a 10 minutos)"
    if (-not ($redisActual -contains $REDIS)) {
        G redis instances create $REDIS --region=$REGION --tier=basic --size=$REDIS_GB --redis-version=$REDIS_VERSION `
            --network=default --labels=$ETIQUETAS --async | Out-Null
        Write-Host "Creación iniciada."
    } else { Write-Host "Ya existe; se reutiliza." }
    Guardar

    if (-not $SinVM) {
        Paso "8. VM e2-micro con redis-cli"
        if (-not $vmActual) {
            $arranque = Join-Path $env:TEMP "lumina_startup.sh"
            "#!/bin/bash`napt-get update -y && apt-get install -y redis-tools`n" | Out-File -Encoding ascii -NoNewline $arranque
            G compute instances create $VM --zone=$ZONA --machine-type=e2-micro --image-family=debian-12 --image-project=debian-cloud `
                --boot-disk-size=10GB --boot-disk-type=pd-standard --labels=$ETIQUETAS --metadata-from-file="startup-script=$arranque" | Out-Null
            Write-Host "VM creada; redis-cli se instala solo en 1 a 2 minutos."
        } else { Write-Host "Ya existe; se reutiliza." }
    }

    Paso "9. Suscripciones y perfiles en Cloud Storage"
    & $PY -m lumina.streaming.publicador_dataflow --preparar
    if ($LASTEXITCODE -ne 0) { throw "Falló la preparación." }
    $ahora = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    foreach ($s in $SUBS) { G pubsub subscriptions seek $s "--time=$ahora" | Out-Null }
    Write-Host "Suscripciones limpias: no se procesarán mensajes viejos."

    Paso "10. Esperando que Memorystore quede READY"
    $limite = (Get-Date).AddMinutes(20)
    do {
        $info = G redis instances describe $REDIS --region=$REGION --format="value(state,host,port)"
        $partes = "$info" -split "\s+"
        Write-Host "  $(Get-Date -Format HH:mm:ss)  $($partes[0])"
        if ($partes[0] -eq "READY") { break }
        Start-Sleep -Seconds 30
    } while ((Get-Date) -lt $limite)
    if ($partes[0] -ne "READY") { throw "Memorystore no quedó READY en 20 minutos." }
    $estado.redis_host = $partes[1]; $estado.redis_port = [int]$partes[2]
    Guardar
    Write-Host "Redis en $($partes[1]):$($partes[2])"

    Paso "11. Lanzando el job de Dataflow"
    if ($jobsActivos) { throw "Ya hay un job activo llamado $JOB ($jobsActivos). Apágalo antes con infra\apagar_y_verificar.ps1." }
    $runId = "df" + $inicio.ToString("yyyyMMddHHmm")
    $salida = & $PY "src\lumina\streaming\dataflow_pipeline.py" --runner DataflowRunner --project $PROYECTO --region $REGION `
        --job_name $JOB --temp_location "gs://$BUCKET/dataflow/temp" --staging_location "gs://$BUCKET/dataflow/staging" `
        --requirements_file "infra\worker-requirements.txt" --num_workers 1 --max_num_workers 1 `
        --autoscaling_algorithm NONE --machine_type $MAQUINA_WORKER --enable_streaming_engine `
        --network default --subnetwork "regions/$REGION/subnetworks/default" --labels proyecto=lumina --labels entorno=demo `
        --redis_host $estado.redis_host --redis_port $estado.redis_port --perfiles_gcs $PERFILES --run_id $runId
    $salida | ForEach-Object { Write-Host $_ }
    $m = $salida | Select-String "^JOB_ID=(.+)$"
    if ($LASTEXITCODE -ne 0 -or -not $m) { throw "No se pudo lanzar el job." }
    $estado.job_id = $m.Matches[0].Groups[1].Value; $estado.run_id = $runId
    Guardar

    Paso "12. Esperando que el job quede Running"
    $limite = (Get-Date).AddMinutes(10)
    do {
        Start-Sleep -Seconds 20
        $st = G dataflow jobs describe $estado.job_id --region=$REGION --format="value(currentState)"
        Write-Host "  $(Get-Date -Format HH:mm:ss)  $st"
    } while ($st -notin @("JOB_STATE_RUNNING", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED") -and (Get-Date) -lt $limite)
    if ($st -ne "JOB_STATE_RUNNING") { throw "El job quedó en $st." }
}
catch {
    Write-Host ""
    Write-Host "ERROR: $_" -ForegroundColor Red
    Write-Host "Puede haber recursos de pago creados. Para no seguir pagando:  .\infra\apagar_y_verificar.ps1" -ForegroundColor Yellow
    exit 1
}

$job = $estado.job_id
Write-Host ""
Write-Host "Despliegue listo. Cobra ~USD $USD_HORA_APROX/h desde $($inicio.ToLocalTime().ToString('HH:mm'))." -ForegroundColor Green
Write-Host "Los workers tardan 2 a 4 minutos más en procesar. Abre el Job Graph y espera a ver los pasos en verde."
Write-Host ""
Write-Host "Consola:"
Write-Host "  Job Graph    https://console.cloud.google.com/dataflow/jobs/$REGION/$($job)?project=$PROYECTO"
Write-Host "  Memorystore  https://console.cloud.google.com/memorystore/redis/locations/$REGION/instances/$REDIS/details/overview?project=$PROYECTO"
if (-not $SinVM) { Write-Host "  VM (SSH)     https://console.cloud.google.com/compute/instancesDetail/zones/$ZONA/instances/$($VM)?project=$PROYECTO" }
Write-Host ""
Write-Host "Siguiente:"
Write-Host "  1. python -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50"
Write-Host "  2. python -m lumina.ops.reporte_dataflow"
Write-Host "  3. .\infra\apagar_y_verificar.ps1"
