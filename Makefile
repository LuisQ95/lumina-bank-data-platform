# Atajos para Linux y macOS. En Windows usa los comandos equivalentes del README.
PY ?= python
export PYTHONPATH := src

.PHONY: install test lint infra fuentes lago carga spark sql consolidar pipeline dataflow-verificar dataflow-desplegar dataflow-publicar dataflow-reporte dataflow-apagar api api-verificar api-desplegar

install:            ## Entorno principal (requiere un venv activo)
	$(PY) -m pip install -e ".[dataflow,api,dev]"

test:               ## Pruebas locales, sin costo
	$(PY) -m pytest

lint:
	$(PY) -m ruff check src tests

infra:              ## 1. Buckets, tópicos, suscripciones y datasets
	$(PY) -m lumina.ingesta.setup_infra

fuentes:            ## 2. Descarga SEC y Trends; genera las fuentes sintéticas en data/
	$(PY) -m lumina.ingesta.generar_fuentes

lago:               ## 3. Sube los Parquet a Cloud Storage y publica eventos de muestra
	$(PY) -m lumina.ingesta.cargar_lago

carga:              ## 4. Carga inicial a BigQuery
	$(PY) -m lumina.ingesta.carga_inicial_bigquery

spark:              ## 5. Pipeline batch en Spark (entorno con Java 17 y pyspark)
	$(PY) -m lumina.batch.spark_cierres

sql:                ## 6. Pipeline SQL Medallion: Bronze -> Silver -> Gold
	$(PY) -m lumina.batch.ejecutar_sql

consolidar:         ## 7. Consultas Gold, medición de clustering y reporte consolidado
	$(PY) -m lumina.pipeline --solo-consolidar

pipeline:           ## 5 a 7 en un solo paso (LUMINA_SPARK_PYTHON apunta al python de Spark)
	$(PY) -m lumina.pipeline

dataflow-verificar: ## 8a. Verifica sin crear nada
	pwsh infra/desplegar_dataflow_memorystore.ps1

dataflow-desplegar: ## 8b. Crea Memorystore, VM y job de Dataflow (cobra por hora)
	pwsh infra/desplegar_dataflow_memorystore.ps1 -AceptoCosto

dataflow-publicar:  ## 8c. Publica tráfico de carga
	$(PY) -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50

dataflow-reporte:   ## 8d. Junta la evidencia antes de apagar
	$(PY) -m lumina.ops.reporte_dataflow

dataflow-apagar:    ## 8e. Elimina todo lo que cobra por hora y verifica
	pwsh infra/apagar_y_verificar.ps1

api:                ## 9a. API local en http://127.0.0.1:8080
	$(PY) -m lumina.api.app

api-verificar:      ## 9b. Pruebas de la API y estado del servicio en Cloud Run
	pwsh infra/desplegar_api.ps1

api-desplegar:      ## 9c. Despliega la API en Cloud Run
	pwsh infra/desplegar_api.ps1 -Desplegar
