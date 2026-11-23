from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Módulo de cruces y fuentes complementarias: PME, exportaciones y demanda SIGEDI.

Tablas producidas (en {catalog}.{schema}, normalmente workspace.alunizaje):
  1. pme_circuito             Medición 15-min en cabecera de circuito (PME) de
     /Volumes/workspace/alunizaje/datos_crudos/PME/ (2025, meses 1-6, ~718k
     filas/mes). Columnas crudas: ANO/MES/HORA/DIA/SUBESTACION/CIRCUITO/
     REGION/ENERGIA_ACTIVA/ENERGIA_REACTIVA/POTENCIA_KW. Aquí se parsea a
     fecha real, bloque 0-95 y kW. El SQL es el mismo del dataset curado del
     dashboard GSPGCF (usa read_files porque los 6 parquet mensuales no
     comparten esquema inferible por spark.read.parquet; _rescued_data filtra
     filas rescatadas).
  2. exportacion_carga        12.071.833 filas, 328 NISEs, 2026-02-01 a
     2026-07-31, 8 OPERACIONES (incluye Proyecto_Activo_kWh, Autoconsumo y
     Generacion SGDA: perfiles con simulación solar). Copia fiel del parquet
     en datos_oficiales/exportaciones/carga.
  3. exportacion_consumo     46.565 filas, 179 NISEs, 2026-06-01 a 2026-07-31,
     operaciones eléctricas (Tension_Fase_A/B/C, Demanda_Delivered/Received).
     Copia fiel del parquet en exportaciones/consumo.
  4. demanda_nacional_comparativa  Limpieza de bronce.demanda_comparativa
     (SIGEDI junio 2026): demanda Nacional vs CNFL a 15-min, bloques 2026 y
     2025 lado a lado. La bronze tiene columnas genéricas _c0.._c7 con
     encabezados embebidos; aquí se renombra, se parsea fecha dd/MM/yyyy
     HH:mm y se castea a double.
  5. demanda_nacional_registrada   Limpieza de bronce.demanda_registrada
     (junio 2026): demanda Nacional/CNFL y Energía Comprada (AAEC y ADMS).
  6. nise_circuito           EL CRUCE. Grano = medidor (NUMEROMEDI). NISE ->
     alimentador (IDALIMENTA vía servicio_baja_tension BT o
     medicion_media_tension MT) -> circuito SIGEL (SIGEL_CircuitSource.ID,
     100% de match en los 118 alimentadores). Validado 2026-11-17:
     640.710 de 647.831 medidores SIGEL matchean BT, 273 MT; 4.704 de 5.212
     NISEs de plata_950_all quedan con alimentador. Un medidor presente en
     varias fuentes queda con UNA fila (prioridad MT, luego el primer
     alimentador alfabético).
  7. circuito_pme_sigel      Mejor esfuerzo por emparejar circuitos PME
     (SUB_XXX.nombre / CLIENTES.x / PLANTA.x) con circuitos SIGEL vía nombre
     normalizado (mayúsculas, sin separadores; el código numérico del ID
     SIGEL se elimina). Grano = (circuito PME, circuito SIGEL): un circuito
     PME puede matchear varios SIGEL con el mismo nombre en subestaciones
     distintas y viceversa; filtra n_matches en el consumidor. Cobertura
     verificada en la tabla tras el update.

Config: project.path_pme_root y project.path_exportaciones (defaults =
volúmenes datos_crudos/PME y datos_oficiales/exportaciones).
"""

PME_ROOT = spark.conf.get(
    "project.path_pme_root", "/Volumes/workspace/alunizaje/datos_crudos/PME"
)
EXP_DIR = spark.conf.get(
    "project.path_exportaciones",
    "/Volumes/workspace/alunizaje/datos_oficiales/exportaciones",
)


# ------------------------------ PME --------------------------------------


@dp.materialized_view(
    name="pme_circuito",
    comment=(
        "Medicion 15-min en cabecera de circuito (PME) 2025 meses 1-6, grano = "
        "circuito x dia x bloque. Parsea dia dd/MM/yyyy + hora HH:mm a fecha y "
        "bloque 0-95; tipo_circuito clasifica SUBESTACION/PLANTA/CLIENTE/OTRO; "
        "nivel1 y nombre_circuito dividen el circuito PME (p.ej. "
        "'SUB_CORONADO.SAN_BLAS'). Fuente: read_files sobre los parquet "
        "mensuales de datos_crudos/PME (esquemas no uniformes entre meses; "
        "_rescued_data descarta filas rescatadas). SQL identico al dataset "
        "curado del dashboard GSPGCF. Une con nise_circuito/circuito_pme_sigel "
        "para comparar demanda de clientes AMI contra su cabecera."
    ),
)
def pme_circuito():
    return spark.sql(
        f"""
        SELECT
          circuito,
          subestacion,
          CASE
            WHEN circuito LIKE 'SUB\\_%' ESCAPE '\\' THEN 'SUBESTACION'
            WHEN upper(circuito) LIKE 'PLANTA%' THEN 'PLANTA'
            WHEN upper(circuito) LIKE 'CLIENTES%' THEN 'CLIENTE'
            ELSE 'OTRO'
          END AS tipo_circuito,
          element_at(split(circuito, '.'), 1) AS nivel1,
          element_at(split(circuito, '.'), 2) AS nombre_circuito,
          to_date(dia, 'dd/MM/yyyy') AS fecha,
          CAST(substring(hora, 1, 2) AS INT) * 4
            + CAST(substring(hora, 4, 2) AS INT) / 15 AS bloque,
          CAST(energia_activa AS DOUBLE) AS energia_activa_kwh,
          CAST(energia_reactiva AS DOUBLE) AS energia_reactiva_kvarh,
          CAST(potencia_kw AS DOUBLE) AS potencia_kw
        FROM read_files('{PME_ROOT}/', format => 'parquet')
        WHERE _rescued_data IS NULL OR _rescued_data = ''
        """
    )


# ------------------------- Exportaciones ---------------------------------


@dp.materialized_view(
    name="exportacion_carga",
    comment=(
        "Copia UC del parquet exportaciones/carga (12.071.833 filas, grano = "
        "NISE x medidor x fecha x operacion). 328 NISEs, 2026-02-01 a "
        "2026-07-31, 8 operaciones incluyendo Proyecto_Activo_kWh / "
        "Proyecto_Reactivo_kVARh y Autoconsumo_SGDA_Activo_kWh (perfiles con "
        "simulacion solar). 220 de sus 328 NISEs estan en plata_950_all."
    ),
)
def exportacion_carga():
    return spark.read.parquet(f"{EXP_DIR}/carga")


@dp.materialized_view(
    name="exportacion_consumo",
    comment=(
        "Copia UC del parquet exportaciones/consumo (46.565 filas, 179 NISEs, "
        "2026-06-01 a 2026-07-31). Operaciones complementarias: "
        "Tension_Fase_A/B/C, Demanda_Delivered/Received_kW, consumo y "
        "generacion SGDA. Grano = NISE x medidor x fecha x operacion."
    ),
)
def exportacion_consumo():
    return spark.read.parquet(f"{EXP_DIR}/consumo")


# ---------------------- Demanda SIGEDI (limpia) --------------------------


@dp.materialized_view(
    name="demanda_nacional_comparativa",
    comment=(
        "Demanda Nacional vs CNFL a 15-min (SIGEDI junio 2026) con bloque 2025 "
        "lado a lado, grano = intervalo. Limpieza de "
        "bronce.demanda_comparativa: la bronze tiene _c0.._c7 genericas con "
        "encabezados embebidos; aqui se filtran solo filas de datos (patron "
        "dd/MM/yyyy HH:mm), se parsean fechas y se castean valores a double. "
        "Util como referencia del patron nacional/CNFL (curva pato) frente a "
        "las curvas de muestra 950."
    ),
)
def demanda_nacional_comparativa():
    return (
        spark.read.table("workspace.bronce.demanda_comparativa")
        .where(F.col("_c0").rlike(r"^\d{2}/\d{2}/\d{4}"))
        .select(
            F.to_timestamp(F.trim("_c0"), "dd/MM/yyyy HH:mm").alias("fecha_2026"),
            F.trim("_c1").cast("double").alias("demanda_nacional_2026"),
            F.trim("_c2").cast("double").alias("demanda_cnfl_2026"),
            F.trim("_c3").cast("double").alias("energia_comprada_2026"),
            F.to_timestamp(F.trim("_c4"), "dd/MM/yyyy HH:mm").alias("fecha_2025"),
            F.trim("_c5").cast("double").alias("demanda_nacional_2025"),
            F.trim("_c6").cast("double").alias("demanda_cnfl_2025"),
            F.trim("_c7").cast("double").alias("energia_comprada_2025"),
            "periodo",
            "mes",
            "file_name",
        )
    )


@dp.materialized_view(
    name="demanda_nacional_registrada",
    comment=(
        "Demanda Nacional/CNFL registrada a 15-min (SIGEDI junio 2026) y "
        "Energia Comprada por AAEC y ADMS, grano = intervalo. Limpieza de "
        "bronce.demanda_registrada (_c0.._c4 genericas): filtra filas de "
        "datos, parsea fecha y castea valores."
    ),
)
def demanda_nacional_registrada():
    return (
        spark.read.table("workspace.bronce.demanda_registrada")
        .where(F.col("_c0").rlike(r"^\d{2}/\d{2}/\d{4}"))
        .select(
            F.to_timestamp(F.trim("_c0"), "dd/MM/yyyy HH:mm").alias("fecha"),
            F.trim("_c1").cast("double").alias("demanda_nacional"),
            F.trim("_c2").cast("double").alias("demanda_cnfl"),
            F.trim("_c3").cast("double").alias("energia_comprada_aaec"),
            F.trim("_c4").cast("double").alias("energia_comprada_adms"),
            "periodo",
            "mes",
            "file_name",
        )
    )


# --------------------------- El cruce ------------------------------------


@dp.materialized_view(
    name="nise_circuito",
    comment=(
        "CRUCE NISE -> medidor -> alimentador -> circuito SIGEL, grano = "
        "medidor (NUMEROMEDI). nise (string, cast desde long) viene de "
        "sigel_infoclientes; id_alimentador de servicio_baja_tension (BT, "
        "clientes de baja tension) o medicion_media_tension (MT, clientes "
        "grandes); nombre_circuito_sigel/subestacion/codigos de "
        "sigel_circuit_source (join IDALIMENTA = ID, 100% de match en los 118 "
        "alimentadores). fuente_alimentador nulo = medidor sin mapeo (~7k de "
        "647.831). Validado 2026-11-17: 4.704 de 5.212 NISEs de plata_950_all "
        "conseguen alimentador. Un medidor con varias fuentes/alimentadores "
        "queda en UNA fila (prioridad MT). Para llegar a la cabecera PME: unir "
        "id_alimentador con circuito_pme_sigel."
    ),
)
def nise_circuito():
    bt = (
        spark.read.table("servicio_baja_tension")
        .where(F.col("IDALIMENTA").isNotNull())
        .select(
            "NUMEROMEDI",
            F.col("IDALIMENTA").alias("id_alimentador"),
            F.lit("BT").alias("fuente_alimentador"),
        )
    )
    mt = (
        spark.read.table("medicion_media_tension")
        .where(F.col("IDALIMENTA").isNotNull())
        .select(
            "NUMEROMEDI",
            F.col("IDALIMENTA").alias("id_alimentador"),
            F.lit("MT").alias("fuente_alimentador"),
        )
    )
    # un medidor puede aparecer en varias fuentes o alimentadores: una sola
    # fila por medidor, priorizando MT (clientes grandes) y luego orden alfabetico
    w = Window.partitionBy("NUMEROMEDI").orderBy(
        F.when(F.col("fuente_alimentador") == "MT", 0).otherwise(1),
        "id_alimentador",
    )
    alim = (
        bt.unionByName(mt)
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select("NUMEROMEDI", "id_alimentador", "fuente_alimentador")
    )
    ic = spark.read.table("sigel_infoclientes").select(
        "NUMEROMEDI", F.col("NISE").cast("string").alias("nise")
    )
    sc = (
        spark.read.table("sigel_circuit_source")
        .select(
            F.col("ID").alias("id_alimentador"),
            F.col("NAME").alias("nombre_circuito_sigel"),
            F.col("SUBSTATION").alias("subestacion_sigel"),
            F.col("CODIGOSUBE").alias("codigo_subestacion"),
            F.col("CODIGOCIRC").alias("codigo_circuito"),
        )
    )
    return (
        ic.join(alim, "NUMEROMEDI", "left")
        .join(sc, "id_alimentador", "left")
        .select(
            "nise",
            F.col("NUMEROMEDI").cast("string").alias("medidor"),
            "fuente_alimentador",
            "id_alimentador",
            "nombre_circuito_sigel",
            "subestacion_sigel",
            "codigo_subestacion",
            "codigo_circuito",
        )
    )


@dp.materialized_view(
    name="circuito_pme_sigel",
    comment=(
        "Emparejamiento mejor-esfuerzo entre circuitos PME (SUB_XXX.nombre, "
        "CLIENTES.x, PLANTA.x) y circuitos SIGEL, grano = (circuito PME, "
        "id_alimentador SIGEL). Match por nombre normalizado: se elimina el "
        "codigo numerico del ID SIGEL (p.ej. '2710 CARGILL' -> 'CARGILL') y se "
        "compara contra nombre_circuito PME en mayusculas sin separadores "
        "('CALLE_RUSIA' -> 'CALLERUSIA'). n_matches>1 = nombre ambiguo en "
        "varias subestaciones; id_alimentador nulo = circuito PME sin "
        "contraparte SIGEL (esperado en trafos/plantas). Cadena completa de "
        "cruce: plata_950_all.nise -> nise_circuito.id_alimentador -> "
        "aqui.circuito -> pme_circuito."
    ),
)
def circuito_pme_sigel():
    pme = (
        spark.read.table("pme_circuito")
        .select("circuito", "subestacion", "nivel1", "nombre_circuito")
        .distinct()
    )
    pme_norm = pme.select(
        F.col("circuito").alias("circuito_pme"),
        F.col("subestacion").alias("subestacion_pme"),
        F.upper(F.regexp_replace(F.col("nivel1"), "^SUB_", "")).alias("nivel_pme"),
        F.upper(F.regexp_replace(F.col("nombre_circuito"), "[^A-Z0-9]", "")).alias(
            "nombre_circuito_norm"
        ),
    )
    sc = (
        spark.read.table("sigel_circuit_source")
        .select(
            F.col("ID").alias("id_alimentador"),
            F.col("NAME").alias("nombre_circuito_sigel"),
            F.upper(
                F.regexp_replace(
                    F.regexp_replace(F.col("ID"), r"^[0-9]+\s*", ""),
                    "[^A-Z0-9]",
                    "",
                )
            ).alias("nombre_sigel_norm"),
            F.col("SUBSTATION").alias("subestacion_sigel"),
            F.col("CODIGOSUBE").alias("codigo_subestacion"),
        )
    )
    return pme_norm.join(
        sc,
        pme_norm.nombre_circuito_norm == sc.nombre_sigel_norm,
        "left",
    ).select(
        "circuito_pme",
        "subestacion_pme",
        "nivel_pme",
        "id_alimentador",
        "nombre_circuito_sigel",
        "subestacion_sigel",
        "codigo_subestacion",
    )
