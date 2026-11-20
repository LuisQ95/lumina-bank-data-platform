-- =====================================================================================
-- PILAR 1 y 3 (ENTREGA P2): PIPELINE SQL MEDALLION  Bronze -> Silver -> Gold
-- Proyecto GCP : lumina-bank-utec-2026 | Región: us-central1
-- Ejecución    : python 07_ejecutar_pipeline_sql.py  (cada bloque "-- @paso:" es un job)
--
-- Regla de diseño: este pipeline NO sobrescribe las tablas de la carga inicial
-- (silver_banco.fact_transaccion y silver_banco.dim_cliente). Lee desde el Data Lake
-- Bronze en Cloud Storage, crea tablas nuevas y al final concilia contra esas tablas.
-- {{SALT}} lo reemplaza el ejecutor con la variable de entorno LUMINA_SALT.
-- =====================================================================================

-- @paso: bronze_ext_clientes
-- Bronze real: tabla externa sobre el lago. Elimina la dependencia circular anterior
-- (core_clientes_cdc_raw era una vista que leía desde Silver).
CREATE OR REPLACE EXTERNAL TABLE `lumina-bank-utec-2026.bronze_banco.gcs_core_clientes`
OPTIONS (format = 'PARQUET', uris = ['gs://lumina-bank-bronze-lumina-bank-utec-2026/cdc_cores/*.parquet']);

-- @paso: bronze_ext_transacciones
CREATE OR REPLACE EXTERNAL TABLE `lumina-bank-utec-2026.bronze_banco.gcs_tx_enriquecidas`
OPTIONS (format = 'PARQUET', uris = ['gs://lumina-bank-bronze-lumina-bank-utec-2026/streaming_sink/tx/*.parquet']);

-- @paso: bronze_ext_sec
CREATE OR REPLACE EXTERNAL TABLE `lumina-bank-utec-2026.bronze_banco.gcs_sec_financials`
OPTIONS (format = 'PARQUET', uris = ['gs://lumina-bank-bronze-lumina-bank-utec-2026/externos/sec/*.parquet']);

-- @paso: bronze_ext_trends
CREATE OR REPLACE EXTERNAL TABLE `lumina-bank-utec-2026.bronze_banco.gcs_google_trends`
OPTIONS (format = 'PARQUET', uris = ['gs://lumina-bank-bronze-lumina-bank-utec-2026/externos/google_trends/*.parquet']);

-- @paso: bronze_ext_edi
CREATE OR REPLACE EXTERNAL TABLE `lumina-bank-utec-2026.bronze_banco.gcs_edi_mercado`
OPTIONS (format = 'PARQUET', uris = ['gs://lumina-bank-bronze-lumina-bank-utec-2026/externos/edi/*.parquet']);

-- @paso: bronze_vista_cdc_sin_circularidad
CREATE OR REPLACE VIEW `lumina-bank-utec-2026.bronze_banco.core_clientes_cdc_raw` AS
SELECT * FROM `lumina-bank-utec-2026.bronze_banco.gcs_core_clientes`;

-- @paso: silver_fx_diario
-- Referencia cambiaria diaria: EDI trae una fila por ticker, se reduce a una por fecha.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.fx_diario` AS
SELECT
    SAFE.PARSE_DATE('%Y-%m-%d', CAST(fecha AS STRING)) AS fecha,
    ROUND(AVG(tipo_cambio_pen_usd), 4) AS tc_pen_usd,
    ROUND(AVG(tasa_referencia_bcrp_pct), 2) AS tasa_referencia_pct
FROM `lumina-bank-utec-2026.bronze_banco.gcs_edi_mercado`
WHERE tipo_cambio_pen_usd IS NOT NULL AND tipo_cambio_pen_usd > 0
GROUP BY 1;

-- @paso: silver_sec_limpia
-- Limpieza real sobre datos reales de la SEC: reportes en varias monedas, acciones,
-- ratios y filas repetidas. Solo se conservan montos en USD de formularios 10-K/10-Q.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.cln_sec_financials` AS
SELECT
    submission_number,
    TRIM(company_name) AS company_name,
    sic,
    form,
    fiscal_year,
    SAFE.PARSE_DATE('%Y%m%d', CAST(period_end_date AS STRING)) AS period_end_date,
    measure_tag,
    value AS value_usd
FROM `lumina-bank-utec-2026.bronze_banco.gcs_sec_financials`
WHERE units = 'USD'
  AND form IN ('10-K', '10-Q')
  AND value IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY submission_number, measure_tag, period_end_date
    ORDER BY ABS(value) DESC
) = 1;

-- @paso: silver_sec_benchmark
-- Benchmark CALCULADO (reemplaza el literal 28.5 de la versión anterior):
-- margen neto = utilidad neta / (ingresos financieros + ingresos no financieros), anual.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.sec_benchmark_bancos` AS
WITH anual AS (
    SELECT
        company_name,
        fiscal_year,
        MAX(IF(measure_tag = 'NetIncomeLoss', value_usd, NULL)) AS utilidad_neta_usd,
        MAX(IF(measure_tag = 'InterestAndDividendIncomeOperating', value_usd, NULL)) AS ingreso_financiero_usd,
        MAX(IF(measure_tag = 'NoninterestIncome', value_usd, NULL)) AS ingreso_no_financiero_usd
    FROM `lumina-bank-utec-2026.silver_banco.cln_sec_financials`
    WHERE form = '10-K'
    GROUP BY 1, 2
),
margenes AS (
    SELECT
        company_name,
        fiscal_year,
        ROUND(SAFE_DIVIDE(utilidad_neta_usd, ingreso_financiero_usd + ingreso_no_financiero_usd) * 100, 2) AS margen_neto_pct
    FROM anual
    WHERE utilidad_neta_usd IS NOT NULL
      AND ingreso_financiero_usd IS NOT NULL
      AND ingreso_no_financiero_usd IS NOT NULL
)
SELECT
    COUNT(*) AS observaciones_empresa_anio,
    COUNT(DISTINCT company_name) AS bancos_referencia,
    MIN(fiscal_year) AS anio_desde,
    MAX(fiscal_year) AS anio_hasta,
    APPROX_QUANTILES(margen_neto_pct, 100)[OFFSET(50)] AS margen_neto_mediana_pct,
    APPROX_QUANTILES(margen_neto_pct, 100)[OFFSET(25)] AS margen_neto_p25_pct,
    APPROX_QUANTILES(margen_neto_pct, 100)[OFFSET(75)] AS margen_neto_p75_pct
FROM margenes
WHERE margen_neto_pct BETWEEN -100 AND 100;

-- @paso: silver_trends_limpia
-- Google Trends público no incluye Perú. Se limpia, se descarta el id_distrito asignado
-- por rotación en el generador y NO se usa para proyectar demanda en Lima.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.cln_google_trends` AS
SELECT
    SAFE.PARSE_DATE('%Y-%m-%d', CAST(refresh_date AS STRING)) AS refresh_date,
    dma_name,
    term,
    score,
    rank
FROM `lumina-bank-utec-2026.bronze_banco.gcs_google_trends`
WHERE score IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY refresh_date, dma_name, term ORDER BY score DESC) = 1;

-- @paso: silver_dim_cliente_segura
-- Golden Record de 9M con validación de máscara sobre TODAS las filas y token SHA-256.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.dim_cliente_segura`
CLUSTER BY id_cliente, id_distrito, id_entidad AS
SELECT
    id_cliente,
    IF(REGEXP_CONTAINS(dni_enmascarado, r'^\*{4}[0-9]{4}$'),
       dni_enmascarado,
       CONCAT('****', RIGHT(dni_enmascarado, 4))) AS dni_enmascarado,
    TO_HEX(SHA256(CONCAT('{{SALT}}', ':', id_cliente))) AS token_identidad_sha256,
    id_entidad,
    id_distrito,
    edad,
    grupo_demografico,
    preferencia_canal,
    segmento,
    score_crediticio,
    ROUND(saldo_disponible_pen, 2) AS saldo_disponible_pen,
    ROUND(limite_diario_pen, 2) AS limite_diario_pen
FROM `lumina-bank-utec-2026.bronze_banco.gcs_core_clientes`
WHERE id_cliente IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY saldo_disponible_pen DESC) = 1;

-- @paso: silver_fact_conformada
-- Cruce heterogéneo principal: Transacciones x Clientes x Distrito x Entidad x Calendario x FX.
-- Clúster encabezado por id_distrito porque las consultas analíticas filtran por distrito.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`
PARTITION BY DATE(timestamp)
CLUSTER BY id_distrito, id_cliente AS
WITH tx AS (
    SELECT *
    FROM `lumina-bank-utec-2026.bronze_banco.gcs_tx_enriquecidas`
    WHERE id_transaccion IS NOT NULL
      AND id_cliente IS NOT NULL
      AND timestamp IS NOT NULL
      AND amount IS NOT NULL
      AND amount >= 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_transaccion ORDER BY timestamp DESC) = 1
)
SELECT
    t.id_transaccion,
    t.timestamp,
    t.fecha,
    t.id_cliente,
    t.id_entidad,
    e.sistema_core_origen,
    t.id_distrito,
    d.nivel_socioeconomico,
    c.id_distrito AS distrito_habitual,
    c.segmento,
    c.grupo_demografico,
    c.edad,
    t.id_atm,
    t.canal,
    UPPER(TRIM(t.type)) AS tipo_operacion,
    ROUND(t.amount, 2) AS monto_pen,
    fx.tc_pen_usd,
    ROUND(SAFE_DIVIDE(t.amount, fx.tc_pen_usd), 2) AS monto_usd,
    ROUND(t.oldbalanceOrg, 2) AS saldo_antes_pen,
    ROUND(t.newbalanceOrig, 2) AS saldo_despues_pen,
    c.limite_diario_pen,
    IF(t.amount > c.limite_diario_pen, 1, 0) AS supera_limite_diario,
    IF(t.id_distrito != c.id_distrito, 1, 0) AS alerta_cambio_distrito,
    COALESCE(cal.nombre_evento, 'Día Operativo Normal') AS evento_calendario,
    COALESCE(cal.factor_multiplicador_liquidez, 1.0) AS factor_calendario,
    t.isFraud AS es_fraude_etiqueta,
    t.isFlaggedFraud AS marcado_sistema
FROM tx t
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_cliente_segura` c ON t.id_cliente = c.id_cliente
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_entidad` e ON t.id_entidad = e.id_entidad
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_distrito` d ON t.id_distrito = d.id_distrito
LEFT JOIN `lumina-bank-utec-2026.silver_banco.fx_diario` fx ON t.fecha = fx.fecha
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_calendario_eventos` cal
    ON t.fecha = SAFE.PARSE_DATE('%Y-%m-%d', cal.fecha);

-- @paso: silver_atm_estado_actual
-- Último estado por ATM: telemetría IoT x catálogo. Mismos umbrales que el flujo streaming.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.atm_estado_actual`
CLUSTER BY id_distrito, id_atm AS
WITH ultima AS (
    SELECT *
    FROM `lumina-bank-utec-2026.bronze_banco.telemetria_atms_iot_raw`
    WHERE id_atm IS NOT NULL AND timestamp IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_atm ORDER BY timestamp DESC) = 1
)
SELECT
    a.id_atm,
    a.id_distrito,
    a.id_entidad_administradora,
    a.modelo_hardware,
    a.capacidad_max_pen,
    u.efectivo_disponible_pen,
    ROUND(SAFE_DIVIDE(u.efectivo_disponible_pen, a.capacidad_max_pen) * 100, 1) AS pct_efectivo,
    u.temperatura_c,
    u.latencia_red_ms,
    u.estado_salud,
    u.timestamp AS ultima_lectura,
    IF(SAFE_DIVIDE(u.efectivo_disponible_pen, a.capacidad_max_pen) * 100 < 20 OR u.temperatura_c > 35, 1, 0) AS requiere_atencion
FROM `lumina-bank-utec-2026.silver_banco.dim_atm` a
LEFT JOIN ultima u ON a.id_atm = u.id_atm;

-- @paso: gold_fraude_agregado
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.gold_fraude.agg_fraude_distrito_dia`
PARTITION BY fecha
CLUSTER BY id_distrito AS
SELECT
    fecha,
    id_distrito,
    nivel_socioeconomico,
    canal,
    COUNT(*) AS tx_evaluadas,
    SUM(es_fraude_etiqueta) AS fraudes_etiquetados,
    SUM(alerta_cambio_distrito) AS alertas_cambio_distrito,
    SUM(supera_limite_diario) AS excesos_limite,
    ROUND(SUM(monto_pen), 2) AS volumen_pen,
    ROUND(SUM(monto_usd), 2) AS volumen_usd,
    ROUND(AVG(monto_pen), 2) AS ticket_promedio_pen
FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`
GROUP BY 1, 2, 3, 4;

-- @paso: gold_liquidez_plan_d1
-- Línea base simple: media móvil de 7 días del retiro en efectivo (CASH_OUT, sin fraude)
-- multiplicada por el factor del calendario del día siguiente. No es un modelo de ML.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.gold_liquidez.plan_liquidez_d1`
CLUSTER BY id_distrito AS
WITH diario AS (
    SELECT
        fecha,
        id_distrito,
        COUNT(*) AS retiros,
        ROUND(SUM(monto_pen), 2) AS demanda_efectivo_pen
    FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`
    WHERE tipo_operacion = 'CASH_OUT' AND es_fraude_etiqueta = 0
    GROUP BY 1, 2
),
base AS (
    SELECT
        *,
        AVG(demanda_efectivo_pen) OVER (
            PARTITION BY id_distrito ORDER BY fecha ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movil_7d_pen,
        COUNT(*) OVER (
            PARTITION BY id_distrito ORDER BY fecha ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS dias_en_ventana
    FROM diario
),
atm AS (
    SELECT id_distrito, COUNT(*) AS cajeros, ROUND(SUM(efectivo_disponible_pen), 2) AS efectivo_en_cajeros_pen
    FROM `lumina-bank-utec-2026.silver_banco.atm_estado_actual`
    GROUP BY 1
)
SELECT
    b.fecha,
    DATE_ADD(b.fecha, INTERVAL 1 DAY) AS fecha_plan,
    b.id_distrito,
    COALESCE(cal.nombre_evento, 'Día Operativo Normal') AS evento_d1,
    COALESCE(cal.factor_multiplicador_liquidez, 1.0) AS factor_d1,
    b.retiros,
    b.demanda_efectivo_pen,
    ROUND(b.media_movil_7d_pen, 2) AS media_movil_7d_pen,
    ROUND(b.media_movil_7d_pen * COALESCE(cal.factor_multiplicador_liquidez, 1.0), 2) AS proyeccion_d1_pen,
    a.cajeros,
    a.efectivo_en_cajeros_pen,
    ROUND(SAFE_DIVIDE(a.efectivo_en_cajeros_pen, b.media_movil_7d_pen * COALESCE(cal.factor_multiplicador_liquidez, 1.0)), 2) AS dias_cobertura,
    ROUND(GREATEST(0, b.media_movil_7d_pen * COALESCE(cal.factor_multiplicador_liquidez, 1.0) - a.efectivo_en_cajeros_pen), 2) AS recarga_sugerida_pen
FROM base b
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_calendario_eventos` cal
    ON DATE_ADD(b.fecha, INTERVAL 1 DAY) = SAFE.PARSE_DATE('%Y-%m-%d', cal.fecha)
LEFT JOIN atm a ON b.id_distrito = a.id_distrito
WHERE b.dias_en_ventana = 7;

-- @paso: gold_operacion_kpi_vs_sec
-- Lee la salida del job Spark (silver_banco.cierres_fx_spark): cierres en USD con FX EDI diario.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.gold_operacion.kpi_operacion_vs_sec`
CLUSTER BY id_entidad, producto_financiero AS
SELECT
    DATE_TRUNC(c.fecha_cierre, MONTH) AS mes,
    c.id_entidad,
    e.sistema_core_origen,
    c.producto_financiero,
    SUM(c.total_transacciones) AS transacciones,
    ROUND(SUM(c.volumen_total_pen), 2) AS volumen_pen,
    ROUND(SUM(c.volumen_usd_edi), 2) AS volumen_usd_edi,
    ROUND(SUM(c.volumen_usd_edi) - SUM(c.volumen_total_usd), 2) AS ajuste_fx_vs_tc_fijo_usd,
    ROUND(SUM(c.ingresos_producto_usd), 2) AS ingresos_usd,
    ROUND(SUM(c.costo_operativo_usd), 2) AS costo_operativo_usd,
    ROUND(SAFE_DIVIDE(SUM(c.costo_operativo_usd), SUM(c.total_transacciones)), 5) AS costo_por_tx_usd,
    ROUND(SAFE_DIVIDE(SUM(c.ingresos_producto_usd) - SUM(c.costo_operativo_usd), SUM(c.ingresos_producto_usd)) * 100, 2) AS margen_operativo_pct,
    ANY_VALUE(b.margen_neto_mediana_pct) AS sec_margen_neto_mediana_pct,
    ANY_VALUE(b.bancos_referencia) AS sec_bancos_referencia
FROM `lumina-bank-utec-2026.silver_banco.cierres_fx_spark` c
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_entidad` e ON c.id_entidad = e.id_entidad
CROSS JOIN `lumina-bank-utec-2026.silver_banco.sec_benchmark_bancos` b
GROUP BY 1, 2, 3, 4;

-- @paso: gold_operacion_salud_atm
-- Cobertura calculada desde GPS. Sin valores por defecto: si no hay datos queda NULL.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.gold_operacion.salud_atm_cobertura`
CLUSTER BY id_distrito AS
WITH gps AS (
    SELECT
        id_atm_mas_cercano AS id_atm,
        COUNT(*) AS lecturas_gps,
        ROUND(AVG(distancia_punto_atencion_km), 2) AS distancia_promedio_km,
        ROUND(COUNTIF(distancia_punto_atencion_km < 8) / COUNT(*) * 100, 1) AS pct_lecturas_menos_8km
    FROM `lumina-bank-utec-2026.bronze_banco.geolocalizacion_gps_raw`
    WHERE distancia_punto_atencion_km IS NOT NULL
    GROUP BY 1
)
SELECT
    a.*,
    g.lecturas_gps,
    g.distancia_promedio_km,
    g.pct_lecturas_menos_8km
FROM `lumina-bank-utec-2026.silver_banco.atm_estado_actual` a
LEFT JOIN gps g ON a.id_atm = g.id_atm;

-- @paso: gold_operacion_experiencia_digital
-- Clickstream x Distrito: fricción digital por grupo de edad.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.gold_operacion.agg_experiencia_digital` AS
SELECT
    k.id_distrito,
    d.nivel_socioeconomico,
    CASE WHEN k.edad_cliente < 30 THEN '18-29' WHEN k.edad_cliente < 60 THEN '30-59' ELSE '60+' END AS grupo_edad,
    k.dispositivo,
    COUNT(*) AS eventos,
    ROUND(AVG(k.tiempo_en_pantalla_seg), 1) AS tiempo_promedio_seg,
    ROUND(AVG(CAST(k.requiere_asesor_humano AS INT64)) * 100, 2) AS pct_requiere_asesor
FROM `lumina-bank-utec-2026.bronze_banco.clickstream_app_web_raw` k
LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_distrito` d ON k.id_distrito = d.id_distrito
GROUP BY 1, 2, 3, 4;

-- @paso: gold_vista_fraude
CREATE OR REPLACE VIEW `lumina-bank-utec-2026.gold_fraude.vw_fraude_tiempo_real_360` AS
SELECT * FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`;

-- @paso: gold_vista_liquidez
CREATE OR REPLACE VIEW `lumina-bank-utec-2026.gold_liquidez.vw_plan_diario_liquidez_d1` AS
SELECT * FROM `lumina-bank-utec-2026.gold_liquidez.plan_liquidez_d1`;

-- @paso: gold_vista_operacion
CREATE OR REPLACE VIEW `lumina-bank-utec-2026.gold_operacion.vw_informes_operativos_vs_sec` AS
SELECT * FROM `lumina-bank-utec-2026.gold_operacion.kpi_operacion_vs_sec`;

-- @paso: gold_vista_atm
CREATE OR REPLACE VIEW `lumina-bank-utec-2026.gold_operacion.vw_mantenimiento_predictivo_atms` AS
SELECT * FROM `lumina-bank-utec-2026.gold_operacion.salud_atm_cobertura`;

-- @paso: calidad_resultados
-- Tabla de auditoría: cuántas filas entran, cuántas salen y cuántas se rechazan por fuente.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.dq_resultados` AS
WITH r AS (
    SELECT 'Transacciones (Bronze GCS -> fact conformada)' AS fuente, 'sintética' AS origen,
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.gcs_tx_enriquecidas`) AS filas_entrada,
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`) AS filas_salida,
           'nulos en llaves, monto negativo, duplicado por id_transaccion' AS reglas
    UNION ALL
    SELECT 'Clientes CDC (Bronze GCS -> dim_cliente_segura)', 'sintética',
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.gcs_core_clientes`),
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.dim_cliente_segura`),
           'id nulo, duplicado por id_cliente, formato de máscara DNI'
    UNION ALL
    SELECT 'SEC Financials (Bronze GCS -> cln_sec_financials)', 'real',
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.gcs_sec_financials`),
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.cln_sec_financials`),
           'solo USD, solo 10-K/10-Q, duplicado por envío + métrica + periodo'
    UNION ALL
    SELECT 'Google Trends (Bronze GCS -> cln_google_trends)', 'real',
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.gcs_google_trends`),
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.cln_google_trends`),
           'score nulo, duplicado por fecha + DMA + término'
    UNION ALL
    SELECT 'EDI mercado (Bronze GCS -> fx_diario)', 'sintética',
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.gcs_edi_mercado`),
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.fx_diario`),
           'agregación de 6 tickers a 1 fila por fecha'
    UNION ALL
    SELECT 'Cierres contables (Spark -> cierres_fx_spark)', 'sintética',
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.cierres_contables_batch_raw`),
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.cierres_fx_spark`),
           'compuerta SHA-256, nulos, volumen <= 0, duplicado por llave de negocio'
    UNION ALL
    SELECT 'Telemetría ATM (Bronze -> atm_estado_actual)', 'sintética',
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.bronze_banco.telemetria_atms_iot_raw`),
           (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.atm_estado_actual`),
           'última lectura por ATM (reducción intencional, no rechazo)'
)
SELECT *, filas_entrada - filas_salida AS filas_excluidas,
       ROUND(SAFE_DIVIDE(filas_entrada - filas_salida, filas_entrada) * 100, 2) AS pct_excluido
FROM r;

-- @paso: calidad_conciliacion
-- Prueba que la salida del pipeline coincide con las tablas de la carga inicial.
CREATE OR REPLACE TABLE `lumina-bank-utec-2026.silver_banco.dq_conciliacion` AS
SELECT
    'fact_transaccion vs fact_transaccion_conformada' AS control,
    (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion`) AS filas_carga_inicial,
    (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`) AS filas_pipeline,
    (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion` a
       LEFT JOIN `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada` b USING (id_transaccion)
      WHERE b.id_transaccion IS NULL OR ABS(a.amount - b.monto_pen) > 0.005) AS filas_con_diferencia,
    (SELECT COUNTIF(distrito_habitual IS NULL) FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`) AS huerfanos_sin_cliente,
    (SELECT COUNTIF(tc_pen_usd IS NULL) FROM `lumina-bank-utec-2026.silver_banco.fact_transaccion_conformada`) AS sin_tipo_cambio
UNION ALL
SELECT
    'dim_cliente vs dim_cliente_segura',
    (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.dim_cliente`),
    (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.dim_cliente_segura`),
    (SELECT COUNT(*) FROM `lumina-bank-utec-2026.silver_banco.dim_cliente` a
       LEFT JOIN `lumina-bank-utec-2026.silver_banco.dim_cliente_segura` b USING (id_cliente)
      WHERE b.id_cliente IS NULL OR a.dni_enmascarado != b.dni_enmascarado),
    (SELECT COUNTIF(NOT REGEXP_CONTAINS(dni_enmascarado, r'^\*{4}[0-9]{4}$')) FROM `lumina-bank-utec-2026.silver_banco.dim_cliente_segura`),
    (SELECT COUNTIF(token_identidad_sha256 IS NULL) FROM `lumina-bank-utec-2026.silver_banco.dim_cliente_segura`);
