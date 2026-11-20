# Atajos para Linux y macOS. En Windows usa los comandos equivalentes del README.
PY ?= python
export PYTHONPATH := src

.PHONY: install test lint infra fuentes lago carga spark sql streaming consolidar pipeline dataflow-verificar dataflow-desplegar dataflow-publicar dataflow-reporte dataflow-apagar

install:            ## Entorno principal (requiere un venv activo)
	$(PY) -m pip install -e ".[dataflow,dev]"

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

streaming:          ## 7. Consumidor streaming local contra Pub/Sub
	$(PY) -m lumina.streaming.consumidor_local --tx 3000 --tasa 50 --streams 8

consolidar:         ## 8. Consultas Gold, medición de clustering y reporte consolidado
	$(PY) -m lumina.pipeline --solo-consolidar

pipeline:           ## 5 a 8 en un solo paso (LUMINA_SPARK_PYTHON apunta al python de Spark)
	$(PY) -m lumina.pipeline

dataflow-verificar: ## 9a. Verifica sin crear nada
	pwsh infra/desplegar_dataflow_memorystore.ps1

dataflow-desplegar: ## 9b. Crea Memorystore, VM y job de Dataflow (cobra por hora)
	pwsh infra/desplegar_dataflow_memorystore.ps1 -AceptoCosto

dataflow-publicar:  ## 9c. Publica el tráfico de prueba
	$(PY) -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50

dataflow-reporte:   ## 9d. Junta la evidencia antes de apagar
	$(PY) -m lumina.ops.reporte_dataflow

dataflow-apagar:    ## 9e. Elimina todo lo que cobra por hora y verifica
	pwsh infra/apagar_y_verificar.ps1
