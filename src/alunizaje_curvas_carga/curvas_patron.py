from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Curvas patron por metodologias documentadas + Q3 del grupo.

Entrada: plata_950_all (operacion Consumo_Activo_kWh, demanda en kW
promedio por bloque de 15 min).

Dimensiones de agrupacion (segun documentacion CNFL):
  - tipo_curva / etiqueta_curva: grupo de curva de infoclientes
    (etiqueta gruesa = la que usa el DMD por tarifa).
  - tipo_dia: ENTRE_SEMANA (lun-vie) / FIN_DE_SEMANA (sab-dom).
  - epoca: LLUVIOSA (1 May - 30 Nov) / SECA (resto)
    (Seleccion_Curvas_DMD.pdf, pag. 7).
  - bloque: 0-95 (intervalo de 15 min del dia).

Metodologias comparadas:
  1. promedio  Promedio del grupo. Metodologia del documento de
               seleccion DMD (pag. 7: "se obtuvo un promedio de las
               curvas en cuestion y se normalizaron").
  2. max_10    Maximo del grupo + 10% (enfoque conservador de
               dimensionamiento).
  3. q3        Percentil 75 del grupo (propuesta del usuario).
  4. iqr_sup   Q3 + 1.5 * IQR (limite superior de Tukey: envelope
               robusto frente a valores atipicos, a mitad de camino
               entre el promedio y el maximo).

Cada fila trae el valor en kW (valor_kw) y la curva normalizada
(valor_pu): dividida por el maximo de la propia metodologia en el
grupo, como exige el DMD (curvas normalizadas por etiqueta).
"""


@dp.materialized_view(
    name="curvas_patron_metodologias",
    comment=(
        "Curvas patron por tipo_curva x tipo_dia x epoca x bloque con "
        "4 metodologias (promedio doc, max+10%, Q3, IQR superior) en kW "
        "y normalizadas p.u. Base para comparar metodologias de "
        "obtencion de curvas patron."
    ),
)
def curvas_patron_metodologias():
    base = (
        spark.read.table("plata_950_all")
        .filter(
            (F.col("operacion") == "Consumo_Activo_kWh")
            & F.col("demanda").isNotNull()
        )
        .withColumn(
            "bloque",
            F.floor(
                (F.hour(F.col("fecha_lectura")) * F.lit(60)
                 + F.minute(F.col("fecha_lectura"))) / F.lit(15)
            ).cast("int"),
        )
        .withColumn(
            "tipo_dia",
            F.when(
                F.dayofweek(F.col("fecha")).isin([1, 7]),
                F.lit("FIN_DE_SEMANA"),
            ).otherwise(F.lit("ENTRE_SEMANA")),
        )
        .withColumn(
            "epoca",
            F.when(
                F.month(F.col("fecha")).between(5, 11),
                F.lit("LLUVIOSA"),
            ).otherwise(F.lit("SECA")),
        )
        .filter(F.col("bloque").between(0, 95))
    )

    stats = (
        base.groupBy(
            "tipo_curva", "etiqueta_curva", "tipo_dia", "epoca", "bloque"
        )
        .agg(
            F.countDistinct("nise").alias("nises_grupo"),
            F.avg("demanda").alias("promedio"),
            F.max("demanda").alias("maximo"),
            F.percentile_approx("demanda", 0.75).alias("q3"),
            F.percentile_approx("demanda", 0.25).alias("q1"),
        )
        .withColumn("max_10", F.col("maximo") * F.lit(1.10))
        .withColumn(
            "iqr_sup",
            F.col("q3") + F.lit(1.5) * (F.col("q3") - F.col("q1")),
        )
    )

    # Una fila por (grupo, bloque, metodologia)
    por_metodologia = stats.select(
        "tipo_curva",
        "etiqueta_curva",
        "tipo_dia",
        "epoca",
        "bloque",
        "nises_grupo",
        F.explode(
            F.create_map(
                F.lit("promedio"), F.col("promedio"),
                F.lit("max_10"), F.col("max_10"),
                F.lit("q3"), F.col("q3"),
                F.lit("iqr_sup"), F.col("iqr_sup"),
            )
        ).alias("metodologia", "valor_kw"),
    )

    # Normalizacion p.u. respecto al maximo de la propia metodologia
    # dentro del grupo (curva normalizada que exige el DMD).
    w_pu = Window.partitionBy(
        "tipo_curva", "etiqueta_curva", "tipo_dia", "epoca", "metodologia"
    )

    return por_metodologia.select(
        "tipo_curva",
        "etiqueta_curva",
        "tipo_dia",
        "epoca",
        "bloque",
        "metodologia",
        F.round("valor_kw", 4).alias("valor_kw"),
        F.round(
            F.col("valor_kw") / F.max("valor_kw").over(w_pu), 6
        ).alias("valor_pu"),
        "nises_grupo",
    )


@dp.materialized_view(
    name="curva_caracteristica_cliente",
    comment=(
        "Curva caracteristica por cliente: mediana de energia (kWh) y demanda "
        "(kW) por bloque de 15 min para cada NISE de plata_950_all "
        "(operacion Consumo_Activo_kWh), separando tipo de dia (ENTRE_SEMANA / "
        "FIN_DE_SEMANA). ~192 filas por NISE (96 bloques x 2 tipos de dia); a "
        "masa completa (~280k NISEs) son ~54M filas. n_dias y rango de fechas "
        "documentan la base de cada mediana. Es la curva individual de cada "
        "cliente Infoclientes con AMI; curvas_patron_metodologias es su "
        "agregado por grupo tipo_curva."
    ),
)
def curva_caracteristica_cliente():
    base = (
        spark.read.table("plata_950_all")
        .filter(
            (F.col("operacion") == "Consumo_Activo_kWh")
            & F.col("energia").isNotNull()
        )
        .withColumn(
            "bloque",
            F.floor(
                (F.hour(F.col("fecha_lectura")) * F.lit(60)
                 + F.minute(F.col("fecha_lectura"))) / F.lit(15)
            ).cast("int"),
        )
        .withColumn(
            "tipo_dia",
            F.when(
                F.dayofweek(F.col("fecha")).isin([1, 7]),
                F.lit("FIN_DE_SEMANA"),
            ).otherwise(F.lit("ENTRE_SEMANA")),
        )
        .filter(F.col("bloque").between(0, 95))
    )

    return (
        base.groupBy(
            "nise", "tipo_curva", "etiqueta_curva", "tarifa", "tipo_dia", "bloque"
        )
        .agg(
            F.percentile_approx("energia", 0.5).alias("energia_mediana_kwh"),
            F.percentile_approx("demanda", 0.5).alias("demanda_mediana_kw"),
            F.countDistinct("fecha").alias("n_dias"),
            F.min("fecha").alias("fecha_min"),
            F.max("fecha").alias("fecha_max"),
        )
    )
