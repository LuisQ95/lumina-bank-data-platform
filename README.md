# Lumina Bank · Plataforma de datos en Google Cloud

Plataforma analítica para la fusión de 12 entidades financieras (caso DataBank Metropolitan → Lumina Bank): 9 millones de clientes, 8 distritos de Lima y transacciones en tiempo real.

- **Batch:** Cloud Storage → Apache Spark → BigQuery con arquitectura Medallion (Bronze, Silver, Gold) y esquema en estrella.
- **Streaming:** Pub/Sub → Cloud Dataflow (Apache Beam) con estado en Memorystore for Redis → BigQuery.
- **Exposición:** API y página web en Cloud Run que publica transacciones reales en Pub/Sub y expone las decisiones y las tablas Gold.
- **FinOps:** los servicios que cobran por hora se crean y eliminan con un solo comando; Cloud Run escala a cero.

![Pipeline de Dataflow con Memorystore](docs/arquitectura/arquitectura_dataflow_memorystore.png)

**Equipo:** Enzo Ochoa, Luis Quispe y Roberto Mendez. Maestría en Ciencia de Datos, UTEC, curso de Big Data (2026).

**API en producción:** https://lumina-api-338829036670.us-central1.run.app

## Estructura

```text
lumina-bank-data-platform/
├── src/lumina/
│   ├── config.py                    Configuración central (variables de entorno / .env)
│   ├── pipeline.py                  Orquestador batch: Spark → SQL → reporte consolidado
│   ├── ingesta/
│   │   ├── setup_infra.py           Buckets, tópicos, suscripciones y datasets
│   │   ├── generar_fuentes.py       Descarga SEC y Google Trends; genera las fuentes sintéticas
│   │   ├── cargar_lago.py           Sube los Parquet a Cloud Storage y publica eventos de muestra
│   │   └── carga_inicial_bigquery.py Carga inicial de Bronze y Silver
│   ├── batch/
│   │   ├── spark_cierres.py         Compuerta SHA-256, limpieza y cruces en Spark
│   │   ├── ejecutar_sql.py          Ejecuta el pipeline SQL paso a paso y registra cada job
│   │   └── sql/medallion.sql        24 pasos Bronze → Silver → Gold, calidad y conciliación
│   ├── streaming/
│   │   ├── dataflow_pipeline.py     Pipeline Apache Beam para Dataflow, estado en Redis
│   │   └── publicador_dataflow.py   Prepara suscripciones y publica tráfico de carga
│   ├── api/
│   │   ├── app.py                   API Flask y página web (Cloud Run)
│   │   ├── consultas.py             Consultas parametrizadas a Silver y Gold con tope de bytes
│   │   ├── eventos.py               Validación y publicación de transacciones en Pub/Sub
│   │   ├── estado_redis.py          Lectura del estado en Memorystore
│   │   ├── templates/ · static/     Página web
│   └── ops/
│       └── reporte_dataflow.py      Junta la evidencia de un despliegue de Dataflow
├── infra/
│   ├── config.ps1                   Nombres, región y tamaños de los recursos
│   ├── desplegar_dataflow_memorystore.ps1
│   ├── desplegar_api.ps1            Cloud Run con cuenta de servicio de permisos mínimos
│   ├── apagar_y_verificar.ps1
│   ├── worker-requirements.txt      Dependencias de cada worker de Dataflow
│   ├── api-requirements.txt         Dependencias de la imagen de Cloud Run
│   └── redis_evidencia.sh           Consultas redis-cli de verificación (se ejecuta en la VM)
├── tests/                           Pruebas locales, sin costo (pytest, fakeredis, cliente Flask)
├── reports/                         Evidencia JSON de las ejecuciones reales en GCP
├── docs/arquitectura/               Diagramas
├── Dockerfile                       Imagen de la API
├── .env.example                     Plantilla de configuración
├── pyproject.toml                   Paquete, dependencias, pytest y ruff
├── requirements.txt                 Entorno principal
├── requirements-spark.txt           Entorno de Spark (Java 17)
└── Makefile                         Atajos para Linux y macOS
```

## 1. Requisitos

| Herramienta | Versión | Para qué |
|---|---|---|
| Python | 3.11 (3.10 a 3.12) | Todo el código |
| Google Cloud CLI (`gcloud`) | reciente | Credenciales y scripts de `infra/` |
| Java | 17 | Solo para Spark |
| PowerShell | 5.1 o 7 | Solo para `infra/` (en Linux/macOS: `pwsh`) |
| Proyecto de GCP | con facturación habilitada | Dataflow y Memorystore no tienen capa gratuita |

## 2. Instalación

```powershell
git clone https://github.com/LuisQ95/lumina-bank-data-platform.git
cd lumina-bank-data-platform

python -m venv .venv
.venv\Scripts\activate                 # Linux/macOS: source .venv/bin/activate
pip install -e ".[dataflow,api,dev]"    # o: pip install -r requirements.txt
```

Spark necesita Java 17, así que va en un entorno aparte:

```powershell
conda create -n lumina-spark -c conda-forge python=3.11 openjdk=17
conda activate lumina-spark
pip install -r requirements-spark.txt
pip install -e . --no-deps
```

## 3. Credenciales y configuración

```powershell
gcloud auth login
gcloud auth application-default login      # credenciales que usan las librerías de Python
gcloud config set project lumina-bank-utec-2026

gcloud services enable storage.googleapis.com pubsub.googleapis.com bigquery.googleapis.com `
    dataflow.googleapis.com compute.googleapis.com redis.googleapis.com

copy .env.example .env                     # Linux/macOS: cp .env.example .env
```

Edita `.env` si tu proyecto o región son distintos. Todas las variables están documentadas en [.env.example](.env.example); el código no tiene rutas ni proyectos escritos a mano.

La cuenta que ejecuta necesita estos roles: Storage Admin, Pub/Sub Editor, BigQuery Admin, Dataflow Admin, Cloud Memorystore Redis Admin, Compute Instance Admin, Cloud Run Admin y Service Account Admin. La cuenta de servicio por defecto de Compute Engine también necesita escribir en BigQuery y Cloud Storage, porque es la que usan los workers de Dataflow. La API usa su propia cuenta, `lumina-api`, que crea `infra/desplegar_api.ps1` con permisos mínimos.

No subas llaves JSON al repositorio: `.gitignore` ya excluye `.env` y `*-key.json`.

## 4. Ejecución, en este orden

Todos los comandos se ejecutan desde la raíz del repositorio con el entorno activado.

### 4.1 Infraestructura y datos (una sola vez)

| Paso | Comando | Resultado | Costo |
|---|---|---|---|
| 1 | `python -m lumina.ingesta.setup_infra` | 2 buckets, 4 tópicos, 4 suscripciones, 5 datasets | Gratis |
| 2 | `python -m lumina.ingesta.generar_fuentes` | 11 fuentes Parquet en `data/` (unos 500 MB) | Gratis |
| 3 | `python -m lumina.ingesta.cargar_lago` | Parquet en Landing y Bronze, eventos de muestra en Pub/Sub | Capa gratuita |
| 4 | `python -m lumina.ingesta.carga_inicial_bigquery` | Tablas Bronze y Silver base (9 M clientes, 6.36 M transacciones) | Capa gratuita |

### 4.2 Pipeline batch

| Paso | Comando | Resultado | Reporte |
|---|---|---|---|
| 5 | `python -m lumina.batch.spark_cierres` *(entorno Spark)* | Compuerta SHA-256 con prueba negativa, cierres × tipo de cambio, transacciones × clientes | `reports/reporte_spark_p2.json` |
| 6 | `python -m lumina.batch.ejecutar_sql` | 24 pasos: tablas externas, Silver conformado, 4 tablas Gold, calidad y conciliación | `reports/reporte_pipeline_sql_p2.json` |
| 7 | `python -m lumina.pipeline --solo-consolidar` | Consultas Gold, medición de partición y clustering, uso frente a cuotas | `reports/reporte_ejecucion_poc_p2.json` |

El orden importa: el SQL lee `silver_banco.cierres_fx_spark`, que produce Spark. Para ejecutar los pasos 5 a 7 de una vez, define `LUMINA_SPARK_PYTHON` con el Python del entorno de Spark y ejecuta `python -m lumina.pipeline`.

```powershell
python -m lumina.batch.ejecutar_sql --dry-run       # estima bytes sin ejecutar
python -m lumina.batch.ejecutar_sql --desde gold_   # retoma desde un paso
python -m lumina.batch.spark_cierres --sin-bigquery # Spark sin cargar a BigQuery
```

### 4.3 Streaming en Dataflow con Memorystore (cobra por hora)

Dataflow y Memorystore cuestan unos USD 0.31 por hora mientras existen. Se crean para la sesión y se eliminan al terminar; Cloud Storage, Pub/Sub y BigQuery se conservan.

| Paso | Comando | Qué hace |
|---|---|---|
| 8a | `.\infra\desplegar_dataflow_memorystore.ps1` | Solo verifica: credenciales, pruebas, armado del job (dry run) y recursos existentes. No crea nada |
| 8b | `.\infra\desplegar_dataflow_memorystore.ps1 -AceptoCosto` | Crea Memorystore (Basic 1 GB), una VM e2-micro para verificar y el job de Dataflow (1 worker). 15 a 25 minutos |
| 8c | `python -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50` | Tráfico de carga: 3,121 transacciones con ráfagas y reintentos duplicados |
| 8d | `python -m lumina.ops.reporte_dataflow` | Guarda la evidencia en `reports/reporte_dataflow_p2.json` |
| 8e | `.\infra\apagar_y_verificar.ps1` | Desconecta la API de la VPC, cancela el job, elimina Memorystore y la VM, y verifica que no quede nada cobrando |

Si el paso 8b falla, ejecuta 8e de inmediato para no seguir pagando.

### 4.4 API y página web en Cloud Run

| Paso | Comando | Qué hace |
|---|---|---|
| 9a | `python -m lumina.api.app` | Servidor local en http://127.0.0.1:8080 contra tu BigQuery y Pub/Sub |
| 9b | `.\infra\desplegar_api.ps1` | Solo verifica: cuenta, pruebas de la API y estado del servicio |
| 9c | `.\infra\desplegar_api.ps1 -Desplegar` | Crea la cuenta de servicio `lumina-api`, construye la imagen y despliega en Cloud Run (máximo 1 instancia, escala a cero) |
| 9d | `.\infra\desplegar_api.ps1 -Desplegar -ConectarMemorystore` | Igual que 9c, con salida directa a la VPC para leer el estado en Memorystore. Requiere el paso 8b |

En Linux o Cloud Shell, el despliegue equivale a:

```bash
gcloud run deploy lumina-api --source . --region us-central1 \
  --service-account lumina-api@lumina-bank-utec-2026.iam.gserviceaccount.com \
  --allow-unauthenticated --max-instances 1 --set-env-vars GCP_PROJECT=lumina-bank-utec-2026
```

Endpoints:

| Método | Ruta | Fuente | Respuesta |
|---|---|---|---|
| GET | `/` | | Página web de la demo |
| GET | `/api/salud` | | Estado del servicio y conexión a Memorystore |
| GET | `/api/clientes` | Cloud Storage | Clientes del core con su límite diario |
| POST | `/api/gps` | Pub/Sub `geo` | Publica la ubicación del celular del cliente |
| POST | `/api/transacciones` | Pub/Sub `transacciones` | Publica una transacción; Dataflow la decide |
| GET | `/api/decisiones/<id>` | Silver | Decisión de una transacción (404 mientras está pendiente) |
| GET | `/api/decisiones/recientes?limite=20` | Silver | Últimas decisiones de Dataflow |
| GET | `/api/decisiones/resumen?horas=24` | Silver | Conteo y latencia p50/p95 por decisión |
| GET | `/api/estado/<id_cliente>` | Memorystore | Acumulado, GPS, ventana de velocidad y clave dedup (503 sin conexión) |
| GET | `/api/fraude/distritos?dias=30` | Gold | Fraude por distrito |
| GET | `/api/liquidez/plan?distrito=Lince` | Gold | Plan de liquidez del día siguiente |
| GET | `/api/operacion/kpi?entidad=Entidad_01` | Gold | KPI operativos frente a la referencia SEC |
| GET | `/api/atm/salud?solo_alertas=1` | Gold | Cajeros que requieren atención |
| GET | `/api/digital/experiencia` | Gold | Fricción digital por distrito y dispositivo |

Ejemplo:

```bash
curl -X POST https://lumina-api-338829036670.us-central1.run.app/api/transacciones \
  -H "Content-Type: application/json" \
  -d '{"id_cliente":"LUM-0013864","id_distrito":"Miraflores","canal":"APP_MOVIL","tipo_operacion":"TRANSFER","monto_pen":1800}'
```

Cada consulta es parametrizada y tiene un tope de 200 MB facturados. La API valida cliente, distrito, canal, tipo y monto antes de publicar.

## 5. Demo de punta a punta

1. Ejecuta el paso 8b unos 25 minutos antes y luego el 9d.
2. Abre la página de la API y elige un cliente. Reporta su ubicación GPS.
3. Envía una transacción en otro distrito y con un monto alto. Pub/Sub la entrega a Dataflow, que lee y actualiza el estado del cliente en Memorystore y escribe la decisión en BigQuery. La página la muestra en segundos.
4. Pulsa "Reenviar la misma": Redis descarta el duplicado y BigQuery sigue con una sola fila.
5. Pulsa "Ráfaga de 4": sube la velocidad del cliente y el riesgo.
6. Muestra el Job Graph en Dataflow y las tablas Gold en la sección 4 de la página.
7. Al terminar, ejecuta el paso 8e.

## 6. Pruebas

```powershell
python -m pytest            # 34 pruebas locales, sin GCP ni costo
python -m ruff check src tests
```

- `tests/test_dataflow_pipeline.py`: reglas de decisión, deduplicación, ventanas por tiempo de evento y el grafo completo con Redis simulado (fakeredis).
- `tests/test_api.py`: validación de transacciones, reintentos, límites de parámetros, endpoints y cabeceras de seguridad, con BigQuery y Pub/Sub reemplazados por dobles.
- `tests/test_sql_medallion.py`: el SQL se divide en pasos únicos, Silver va antes que Gold y existen las cuatro tablas Gold.
- `tests/test_config.py`: la configuración se toma de variables de entorno.

## 7. Resultados medidos

La carpeta `reports/` guarda la evidencia de las ejecuciones reales en `lumina-bank-utec-2026`:

| Reporte | Resultado principal |
|---|---|
| `reporte_spark_p2.json` | Compuerta SHA-256 aprobada; la prueba negativa bloquea el lote alterado |
| `reporte_pipeline_sql_p2.json` | 24 jobs, conciliación con 0 diferencias en 15.36 M de filas |
| `reporte_dataflow_p2.json` | Job de Dataflow con 3,060 decisiones, 61 duplicados filtrados en Redis, operaciones Redis p50 21 ms |
| `reporte_publicador_dataflow.json` | Tráfico de carga publicado: tasa real, ráfagas y reintentos |
| `reporte_dataflow_p3.json` | Demo desde la API: 20 transacciones reales, evento → decisión 411 ms p50, Redis 2.1 ms p50, 0 duplicados |
| `reporte_demo_api_p3.json` | Resumen de la demo: decisiones por tipo, reintento con una sola fila y consultas Gold en 0.77 s |
| `redis_cli_evidencia.txt` | Conexión activa a Memorystore desde la VM: 12,303 claves escritas por Dataflow |
| `reporte_ejecucion_poc_p2.json` | Consolidado: consultas Gold, partición y clustering, uso frente a cuotas |

## 8. Datos

SEC Financials y Google Trends provienen de datasets públicos de BigQuery. Las demás fuentes (transacciones con esquema PaySim, clientes, cierres, tipo de cambio EDI, GPS, cajeros y clics) son sintéticas y se generan con una semilla fija, por lo que la ejecución es reproducible. Los datos de clientes son sintéticos porque la Ley 29733 de Protección de Datos Personales impide usar datos reales de clientes bancarios. La carpeta `data/` no se sube al repositorio.

## 9. Problemas comunes

| Síntoma | Solución |
|---|---|
| `DefaultCredentialsError` | Ejecuta `gcloud auth application-default login` |
| Spark: `JAVA_HOME is not set` | Activa el entorno `lumina-spark`, que incluye Java 17 |
| `infra/*.ps1` no se ejecuta en Windows | `Set-ExecutionPolicy -Scope Process Bypass` |
| `gcloud` no se encuentra | Agrégalo al PATH o define `GCLOUD_BIN` en `.env` |
| La página dice "No llegó la decisión" | El job de Dataflow no está corriendo: ejecuta el paso 8b |
| `/api/estado` responde 503 | La API no está conectada a Memorystore: ejecuta el paso 9d |
