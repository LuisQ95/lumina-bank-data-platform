"""El archivo SQL Medallion se divide en pasos ejecutables y en el orden correcto, sin tocar BigQuery."""

from lumina.batch import ejecutar_sql


def pasos():
    return ejecutar_sql.leer_pasos()


def test_hay_pasos_y_nombres_unicos():
    p = pasos()
    nombres = [n for n, _ in p]
    assert len(p) >= 20
    assert len(nombres) == len(set(nombres))


def test_cada_paso_tiene_sql_y_sin_marcadores_pendientes():
    for nombre, sql in pasos():
        assert sql.strip(), nombre
        assert "{{SALT}}" not in sql, nombre


def test_silver_antes_que_gold():
    nombres = [n for n, _ in pasos()]
    primero_gold = next(i for i, n in enumerate(nombres) if n.startswith("gold_"))
    assert all(not n.startswith("gold_") for n in nombres[:primero_gold])
    assert any(n.startswith("silver_") for n in nombres[:primero_gold])


def test_gold_crea_las_cuatro_verticales():
    sql = "\n".join(s for _, s in pasos())
    for tabla in ("gold_fraude.agg_fraude_distrito_dia", "gold_liquidez.plan_liquidez_d1",
                  "gold_operacion.kpi_operacion_vs_sec", "gold_operacion.salud_atm_cobertura"):
        assert tabla in sql, tabla
