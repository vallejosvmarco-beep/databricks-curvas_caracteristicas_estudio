from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Replica all-NISEs de la cadena de calidad hasta el ensamblaje estilo plata_950.

Fuentes (todas existentes en el catalogo configurado):
  - bronce.carga: lecturas AMI de TODA la base (no solo TMT/TMTB).
  - infoclientes (ArcGIS): catalogo con tipo_curva y tarifa por NISE.
  - plata_analisis.plata_carga_diferencias: pilar "diferencias" existente
    (solo cubre TMT/TMTB, ~186 NISEs); se reutiliza tal cual.

Datasets producidos (en {catalog}.{schema_alunizaje}):
  1. catalogo_clientes_ami        Todos los NISEs con datos AMI en la
                                  ventana, con su grupo de curva.
  2. muestra_clientes_curvas     Muestra estratificada de N clientes por
                                  categoria tipo_curva.
                                  muestra_por_categoria = 999999 equivale
                                  a toda la masa (requiere Full Refresh).
  3. plata_carga_all             Base estilo plata_carga con TODOS los
                                  NISEs: mapeo Delivered/Received x
                                  kWh/kVARh a nombres plata_950 y
                                  DEMANDA = ENERGIA * 60 / INTERVALO.
  4. plata_carga_completitud_all Dedup por (nise, operacion, fecha,
                                  timestamp) y completitud >= 95%
                                  (esperados = dias_rango * 96).
  5. plata_950_all               Ensamblaje 950: pilar diferencias FULL
                                  OUTER pilar completitud con preferencia
                                  "diferencias", y regla todas-las-
                                  operaciones (todo-o-nada por NISE).

Validado con datos reales (13-11-2026, muestra de 2 por categoria):
  396 NISEs en muestra -> 347 con completitud >= 95% -> 342 en plata_950_all.

Unidades: ENERGIA = kWh por intervalo de 15 min (ya es energia, no
requiere *0.25), DEMANDA = kW promedio del intervalo.
"""

SOURCE_CATALOG = spark.conf.get("project.source_catalog")

SCHEMA_CARGA = spark.conf.get("project.source_schema_carga")
SCHEMA_INFOCLIENTES = spark.conf.get("project.source_schema_infoclientes")
TABLE_INFOCLIENTES = spark.conf.get("project.source_table_infoclientes")
SCHEMA_DIFERENCIAS = spark.conf.get("project.source_schema_diferencias")
TABLE_DIFERENCIAS = spark.conf.get("project.source_table_diferencias")

PERIODO_INICIO = spark.conf.get("project.periodo_inicio")
PERIODO_FIN = spark.conf.get("project.periodo_fin")
MUESTRA_POR_CATEGORIA = int(spark.conf.get("project.muestra_por_categoria", "2"))

CARGA_FQN = f"{SOURCE_CATALOG}.{SCHEMA_CARGA}.carga"
INFOCLIENTES_FQN = f"{SOURCE_CATALOG}.{SCHEMA_INFOCLIENTES}.{TABLE_INFOCLIENTES}"
DIFERENCIAS_FQN = f"{SOURCE_CATALOG}.{SCHEMA_DIFERENCIAS}.{TABLE_DIFERENCIAS}"

# Ventana temporal: [periodo_inicio, periodo_fin] inclusive en fechas;
# en timestamps el fin se vuelve exclusivo (+1 dia).
INICIO_TS = F.lit(PERIODO_INICIO).cast("timestamp")
FIN_TS_EXCL = F.date_add(F.lit(PERIODO_FIN).cast("date"), 1).cast("timestamp")
FECHA_INICIO = F.lit(PERIODO_INICIO).cast("date")
FECHA_FIN = F.lit(PERIODO_FIN).cast("date")

# Las 3 operaciones que la regla 950 exige pasar con calidad
OPERACIONES_CALIDAD = [
    "Consumo_Activo_kWh",
    "Inyeccion_Activo_kWh",
    "Consumo_Reactivo_kVARh",
]


@dp.materialized_view(
    name="catalogo_clientes_ami",
    comment=(
        "Todos los NISEs con datos AMI en la ventana y su grupo de curva "
        "(tipo_curva, etiqueta, tarifa) segun infoclientes. Base poblacional "
        "del pipeline all-NISEs."
    ),
)
def catalogo_clientes_ami():
    carga = spark.read.table(CARGA_FQN)

    ami_meds = (
        carga.filter(
            (F.col("FECHA_LECTURA_REAL") >= INICIO_TS)
            & (F.col("FECHA_LECTURA_REAL") < FIN_TS_EXCL)
        )
        .select(F.col("MEDIDOR").alias("med"))
        .distinct()
    )

    info = spark.read.table(INFOCLIENTES_FQN)

    return (
        info.join(
            ami_meds,
            F.col("numero_medidor").cast("string") == F.col("med"),
            "inner",
        )
        .filter(
            F.col("nise").isNotNull()
            & F.col("numero_medidor").isNotNull()
            & F.col("tipo_curva").isNotNull()
            & (F.trim(F.col("tipo_curva")) != "")
            & (F.col("estado_servicio") == "Activo")
        )
        # nise viaja como STRING para alinear con NISE (STRING) del pilar
        # diferencias (verificado: 206/209 TMT cruzan con este cast)
        .select(
            F.col("nise").cast("string").alias("nise"),
            F.col("numero_medidor").cast("string").alias("medidor"),
            F.col("tarifa"),
            F.col("tipo_curva"),
            F.split(F.col("tipo_curva"), " - ").getItem(0).alias("etiqueta_curva"),
            F.col("medidor_ami"),
            F.col("estado_servicio"),
        )
        .distinct()
    )


@dp.materialized_view(
    name="muestra_clientes_curvas",
    comment=(
        "Muestra estratificada de N clientes por categoria tipo_curva "
        "(muestra_por_categoria). Con 999999 cubre toda la masa."
    ),
)
def muestra_clientes_curvas():
    # Orden numerico (nise es BIGINT en origen): el orden lexicografico
    # del string ("100" < "99") cambia que clientes entran a la muestra.
    w = Window.partitionBy("tipo_curva").orderBy(F.col("nise").cast("long"))
    return (
        spark.read.table("catalogo_clientes_ami")
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= MUESTRA_POR_CATEGORIA)
        .select("nise", "medidor", "tarifa", "tipo_curva", "etiqueta_curva")
    )


@dp.materialized_view(
    name="plata_carga_all",
    comment=(
        "Base estilo plata_carga con TODOS los NISEs de la poblacion "
        "(muestra o masa): lecturas AMI mapeadas a nombres plata_950. "
        "DEMANDA = ENERGIA * 60 / INTERVALO (kW promedio del intervalo)."
    ),
)
def plata_carga_all():
    poblacion = spark.read.table("muestra_clientes_curvas").select(
        F.col("nise"),
        F.col("medidor").alias("medidor_p"),
        F.col("tarifa"),
        F.col("tipo_curva"),
        F.col("etiqueta_curva"),
    )

    carga = spark.read.table(CARGA_FQN).filter(
        (F.col("FECHA_LECTURA_REAL") >= INICIO_TS)
        & (F.col("FECHA_LECTURA_REAL") < FIN_TS_EXCL)
        & F.col("OPERACION").isin("Delivered", "Received")
        & F.col("UNIDAD").isin("kWh", "kVARh")
        & (F.col("ENERGIA").isNotNull() | F.col("DEMANDA").isNotNull())
    )

    base = carga.join(poblacion, F.col("MEDIDOR") == F.col("medidor_p"), "inner")

    energia = F.col("ENERGIA").cast("double")

    return base.select(
        F.col("nise"),
        F.col("tipo_curva"),
        F.col("etiqueta_curva"),
        F.col("tarifa"),
        F.col("MEDIDOR").alias("medidor"),
        F.col("FECHA_LECTURA_REAL").alias("fecha_lectura"),
        F.to_date(F.col("FECHA_LECTURA_REAL")).alias("fecha"),
        F.when(
            (F.col("OPERACION") == "Delivered") & (F.col("UNIDAD") == "kWh"),
            F.lit("Consumo_Activo_kWh"),
        )
        .when(
            (F.col("OPERACION") == "Delivered") & (F.col("UNIDAD") == "kVARh"),
            F.lit("Consumo_Reactivo_kVARh"),
        )
        .when(
            (F.col("OPERACION") == "Received") & (F.col("UNIDAD") == "kWh"),
            F.lit("Inyeccion_Activo_kWh"),
        )
        .when(
            (F.col("OPERACION") == "Received") & (F.col("UNIDAD") == "kVARh"),
            F.lit("Inyeccion_Reactivo_kVARh"),
        )
        .alias("operacion"),
        F.col("SGDA").alias("sgda_original"),
        F.col("VALOR_LECTURA").cast("double").alias("valor_lectura"),
        F.col("CONSTANTE"),
        F.col("TIPO_MEDIDOR").alias("tipo_medidor"),
        F.col("INTERVALO").alias("intervalo"),
        energia.alias("energia"),
        F.when(
            F.col("INTERVALO") > 0,
            energia * (F.lit(60.0) / F.col("INTERVALO").cast("double")),
        ).alias("demanda"),
    )


@dp.materialized_view(
    name="plata_carga_completitud_all",
    comment=(
        "Verificacion de completitud estilo plata_950: dedup por (nise, "
        "operacion, fecha, timestamp) y pct_completitud = presentes / "
        "(dias_rango * 96) con flag >= 95%."
    ),
)
def plata_carga_completitud_all():
    base = spark.read.table("plata_carga_all").filter(
        F.col("nise").isNotNull()
        & F.col("operacion").isNotNull()
        & F.col("fecha_lectura").isNotNull()
        & (F.col("demanda").isNotNull() | F.col("energia").isNotNull())
    )

    # Dedup: en bronce cada lectura llega duplicada; se conserva la de
    # mayor DEMANDA/ENERGIA (misma particion que la vista original)
    w_dup = Window.partitionBy("nise", "operacion", "fecha", "fecha_lectura").orderBy(
        F.col("demanda").desc_nulls_last(), F.col("energia").desc_nulls_last()
    )
    lecturas = (
        base.withColumn("dup_rn", F.row_number().over(w_dup))
        .filter(F.col("dup_rn") == 1)
        .drop("dup_rn")
    )

    general = (
        lecturas.groupBy("nise", "operacion")
        .agg(
            F.count(F.lit(1)).alias("datos_presentes"),
            F.min("fecha").alias("fecha_min"),
            F.max("fecha").alias("fecha_max"),
        )
        .withColumn(
            "dias_rango", F.datediff(F.col("fecha_max"), F.col("fecha_min")) + 1
        )
        .withColumn("datos_esperados", F.col("dias_rango") * 96)
        .withColumn(
            "pct_completitud",
            F.round(F.col("datos_presentes") * 100.0 / F.col("datos_esperados"), 2),
        )
        .withColumn(
            "flag_completitud_general_95",
            F.when(F.col("pct_completitud") >= 95.0, F.lit(1)).otherwise(F.lit(0)),
        )
    )

    return lecturas.join(general, ["nise", "operacion"]).select(
        F.col("nise"),
        F.col("operacion"),
        F.col("fecha_lectura"),
        F.col("fecha"),
        F.col("energia"),
        F.col("demanda"),
        F.col("datos_presentes"),
        F.col("datos_esperados"),
        F.col("pct_completitud"),
        F.col("flag_completitud_general_95"),
    )


@dp.materialized_view(
    name="plata_950_all",
    comment=(
        "Ensamblaje estilo plata_950 con TODOS los NISEs: pilar diferencias "
        "(solo TMT/TMTB, vista existente) FULL OUTER pilar completitud con "
        "preferencia 'diferencias', regla todas-las-operaciones por NISE, "
        "y categorias de infoclientes en lugar de PLAN."
    ),
)
def plata_950_all():
    # Pilar 1: diferencias (solo existe para TMT/TMTB)
    dif_ok = (
        spark.read.table(DIFERENCIAS_FQN)
        .filter(F.abs(F.col("PORCENTAJE_DIFERENCIA")) <= 5.0)
        .select(
            F.col("NISE").alias("nise_d"),
            F.col("OPERACION").alias("operacion_d"),
            F.col("FECHA_LECTURA").alias("fecha_lectura"),
            F.col("FECHA").alias("fecha"),
            F.col("ENERGIA").cast("double").alias("energia"),
            F.col("DEMANDA").cast("double").alias("demanda"),
        )
    )

    # Pilar 2: completitud
    comp_ok = spark.read.table("plata_carga_completitud_all").filter(
        F.col("flag_completitud_general_95") == 1
    )

    # Fuente por par (nise, operacion): prefiere diferencias si existe
    d_pairs = dif_ok.select("nise_d", "operacion_d").distinct().withColumn(
        "es_dif", F.lit(True)
    )
    c_pairs = comp_ok.select("nise", "operacion").distinct().withColumn(
        "es_comp", F.lit(True)
    )
    fuente = (
        d_pairs.join(
            c_pairs,
            (F.col("nise_d") == F.col("nise"))
            & (F.col("operacion_d") == F.col("operacion")),
            "full_outer",
        )
        .select(
            F.coalesce(F.col("nise_d"), F.col("nise")).alias("nise"),
            F.coalesce(F.col("operacion_d"), F.col("operacion")).alias("operacion"),
            F.when(F.col("nise_d").isNotNull(), F.lit("diferencias"))
            .otherwise(F.lit("completitud"))
            .alias("fuente_elegida"),
        )
    )

    # Regla 950 (todo-o-nada): el NISE debe pasar calidad en TODAS sus
    # operaciones de las 3 de calidad. Nota: ops_calidad cuenta todas las
    # operaciones presentes (incluida Inyeccion_Reactivo), igual que la
    # vista original: la igualdad estricta es parte de la regla.
    base_all = spark.read.table("plata_carga_all")

    total_ops = (
        base_all.filter(F.col("operacion").isin(OPERACIONES_CALIDAD))
        .groupBy("nise")
        .agg(F.countDistinct("operacion").alias("total_ops"))
    )
    ops_calidad = fuente.groupBy("nise").agg(
        F.countDistinct("operacion").alias("ops_calidad")
    )
    nises_completos = (
        total_ops.join(ops_calidad, "nise")
        .filter(F.col("total_ops") == F.col("ops_calidad"))
        .select("nise")
    )

    catalogo = base_all.groupBy("nise").agg(
        F.min("tipo_curva").alias("tipo_curva"),
        F.min("etiqueta_curva").alias("etiqueta_curva"),
        F.min("tarifa").alias("tarifa"),
        F.min("medidor").alias("medidor"),
    )

    # Lecturas finales: cada par toma lecturas de su fuente elegida
    fd = (
        fuente.filter(F.col("fuente_elegida") == "diferencias")
        .join(nises_completos, "nise")
        .join(catalogo, "nise")
        .join(
            dif_ok,
            (F.col("nise") == F.col("nise_d"))
            & (F.col("operacion") == F.col("operacion_d")),
            "inner",
        )
    )
    fc = (
        fuente.filter(F.col("fuente_elegida") == "completitud")
        .join(nises_completos, "nise")
        .join(catalogo, "nise")
        .join(
            comp_ok.select(
                "nise", "operacion", "fecha_lectura", "fecha", "energia", "demanda"
            ),
            ["nise", "operacion"],
        )
    )

    columnas = (
        "nise",
        "tipo_curva",
        "etiqueta_curva",
        "tarifa",
        "medidor",
        "operacion",
        "fecha_lectura",
        "fecha",
        "energia",
        "demanda",
    )

    fd_rows = fd.select(*columnas, F.lit("diferencias").alias("fuente_calidad"))
    fc_rows = fc.select(*columnas, F.lit("completitud").alias("fuente_calidad"))

    return (
        fd_rows.unionByName(fc_rows)
        .filter(F.col("fecha_lectura").isNotNull())
        .filter((F.col("fecha") >= FECHA_INICIO) & (F.col("fecha") <= FECHA_FIN))
    )
