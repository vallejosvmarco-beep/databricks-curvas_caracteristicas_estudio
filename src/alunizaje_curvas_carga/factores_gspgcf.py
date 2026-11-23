from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F

"""
Factor de correccion GSPGCF por NISE individual (FASE 4).

El factor por celda (tipo_curva x tipo_dia x epoca) falla en clientes
atipicos (peores casos FASE 2/3: clientes grandes TG/TPROM con factores
0.09-0.36). Este modulo produce el factor por NISE individual con
fallback a celda, para que el pipeline consumidor pueda perfilar
cualquier NISE en cualquier fecha.

Metodologia (GSPGCF validado FASE 1-3, mejora global MAE 10.6%
out-of-sample):
  factor_gspgcf_nise = SUM(demanda real del dia)
                      / SUM(valor_pu x p_average_ref_30d_nise)

  p_average_ref_30d_nise = promedio de demanda del NISE en sus ~30
  dias completos PREVIOS (media movil, solo dias con >=91 bloques).

  NOTA CRITICA (hallazgo 16-Nov-2026): la base de escalado NO puede
  ser el promedio del propio dia (P_average_dia). En ese caso el
  factor colapsa matematicamente a bloques_dia / SUM(valor_pu),
  identico para todos los NISEs de la celda (std intra-celda medida:
  0.0003) y NO corrige clientes atipicos. La FASE 1 original uso un
  nivel estable por NISE (constante en el tiempo, verificado en
  validacion_factor_correccion), y el consumidor (Paso 4) usa
  AVG(demanda_real) de ultimos 30 dias: por eso el factor se calibra
  contra la media movil de ~30 dias previos. Los primeros ~20-30 dias
  de cada NISE quedan sin factor (calentamiento de la media movil).

  CRITICO (bug documentado en FASE 1): la curva perfilada usa
  valor_pu x P_average, NUNCA valor_kw x P_average (el doble escalado
  produce factores absurdos 0.03-4.8).

Datasets producidos (en {catalog}.{schema_alunizaje}):
  1. factores_correccion_nise_historicos  Tabla B: factor por
     nise x fecha usando la curva de la metodologia ganadora de la
     celda (FASE 3: curvas_patron_metodologia_ganadora, 396 celdas).
  2. factores_correccion_celda_fallback   Tabla C: mediana por celda
     para NISEs sin historial suficiente (<30 dias).
  3. factores_correccion_celda_fecha       Absorbe factores_correccion_historicos
     (FASE 3.2): factor promedio por celda x fecha sobre todos los
     NISE-dias (el ad-hoc usaba 1 dia determinista por NISE).
  4. validacion_factores_nise (MV de       Distribucion tipico/atipico
     validacion, consultable)                y cobertura factor propio vs
                                             fallback.

Reglas (verificadas con datos):
  - operacion = Consumo_Activo_kWh (demanda en kW promedio por bloque
    de 15 min).
  - Completitud >= 95%: al menos 91 de 96 bloques por nise x fecha.
  - Ventana: TODA la data disponible del pipeline (feb-jul 2026); las
    fuentes AMI estan congeladas desde 1-Sep, NO aplicar "ultimos N
    dias" desde hoy (devolveria vacio).
  - tipo_dia: lun-vie = ENTRE_SEMANA, sab-dom = FIN_DE_SEMANA.
  - epoca: meses 5-11 = LLUVIOSA, resto = SECA (Seleccion_Curvas_DMD
    pag. 7).
  - Se excluyen casos con demanda perfilada <= 0 (incluye los grupos
    degenerados RANGO DE 0-20 con Q3=0, cuyo valor_pu es indefinido).
  - Dedup por (nise, fecha, bloque): el ensamblaje 950 puede traer
    lecturas duplicadas (verificado 2026-03-04).
  - Nivel de referencia: media movil de ~30 dias completos previos
    por NISE, minimo 20 dias de historial (los primeros dias de la
    ventana quedan sin factor mientras la media movil calienta).

La aplicacion del factor (mediana de 90 dias por nise x tipo_dia x
epoca, minimo 30 dias, sino fallback celda, clip 0.5-2.0 con auditoria
fuente_factor) es responsabilidad del pipeline CONSUMIDOR; aqui solo
se producen las tablas que la soportan.
"""

SOURCE_CATALOG = spark.conf.get("project.source_catalog")
SCHEMA_ALUNAJE = spark.conf.get("project.schema_alunizaje")

# Decision metodologica aprobada en FASE 3 (GSPGCF supero el umbral de
# 5% de mejora): metodologia ganadora por celda segun validacion
# out-of-sample. Tabla ad-hoc CONGELADA: el split train/test exacto de
# FASE 2 no es reproducible byte a byte (el codigo ad-hoc no se guardo
# en el workspace), por lo que se consume como insumo congelado.
GANADORA_FQN = (
    f"{SOURCE_CATALOG}.{SCHEMA_ALUNAJE}.curvas_patron_metodologia_ganadora"
)

# 95% de 96 bloques = 91.2 -> minimo 91 bloques presentes
BLOQUES_MINIMOS = 91


@dp.materialized_view(
    name="factores_correccion_nise_historicos",
    comment=(
        "Tabla B: factor de correccion GSPGCF historico por NISE "
        "individual (grano nise x tipo_curva x etiqueta_curva x fecha). "
        "factor_gspgcf_nise = SUM(demanda_real_total_dia_kw) / "
        "SUM(valor_pu x p_average_ref_30d_nise), donde "
        "p_average_ref_30d_nise es el promedio de demanda del NISE en "
        "sus ~30 dias completos previos (media movil, base consistente "
        "con el Paso 4 del consumidor; NO usar el promedio del propio "
        "dia, el factor degeneraria). Calculado con la curva patron "
        "de la metodologia ganadora de la celda (FASE 3, 396 celdas) y la "
        "regla anti doble-escalado (NUNCA valor_kw x P_average). Requisitos: "
        "completitud >=95% (>=91 de 96 bloques), >=20 dias de "
        "historial previo, operacion "
        "Consumo_Activo_kWh, ventana = toda la data disponible (feb-jul "
        "2026). Incluye demanda_perfilada_total_dia_kw (con base de "
        "referencia) y p_average_dia_kw para auditoria. "
        "Usarlo para perfilar cualquier NISE en cualquier fecha: mediana "
        "de ~90 dias por nise x tipo_dia x epoca con minimo 30 dias; si "
        "no, factores_correccion_celda_fallback. Solo cubre NISEs de "
        "celdas con metodologia ganadora (categorias con >=30 curvas)."
    ),
)
def factores_correccion_nise_historicos():
    # Metodologia ganadora por celda (decision FASE 3, congelada)
    ganadora = spark.read.table(GANADORA_FQN).select(
        "tipo_curva", "tipo_dia", "epoca", "metodologia_ganadora"
    )

    # Curva patron (Tabla A) reducida a la metodologia ganadora de cada celda
    curvas = (
        spark.read.table("curvas_patron_metodologias")
        .join(ganadora, ["tipo_curva", "tipo_dia", "epoca"], "inner")
        .filter(F.col("metodologia") == F.col("metodologia_ganadora"))
        .select(
            "tipo_curva",
            "tipo_dia",
            "epoca",
            F.col("bloque").cast("int").alias("bloque"),
            "metodologia_ganadora",
            "valor_pu",
            "nises_grupo",
        )
    )

    lecturas = (
        spark.read.table("plata_950_all")
        .filter(
            (F.col("operacion") == "Consumo_Activo_kWh")
            & F.col("demanda").isNotNull()
        )
        .withColumn(
            "bloque",
            F.floor(
                (F.hour(F.col("fecha_lectura")) * F.lit(60)
                 + F.minute(F.col("fecha_lectura")))
                / F.lit(15)
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
        # Dedup defensivo: el ensamblaje 950 puede traer duplicados
        .dropDuplicates(["nise", "fecha", "bloque"])
    )

    # Nivel de referencia del NISE: media movil de ~30 dias completos
    # PREVIOS (misma base que el Paso 4 del consumidor). NO usar el
    # promedio del propio dia: el factor colapsaria a
    # bloques_dia / SUM(valor_pu), igual para toda la celda.
    dias_nise = (
        lecturas.groupBy("nise", "fecha")
        .agg(
            F.count(F.lit(1)).alias("bloques_hist"),
            F.avg("demanda").alias("p_average_dia_hist"),
        )
        .filter(F.col("bloques_hist") >= BLOQUES_MINIMOS)
    )

    ventana_ref = (
        Window.partitionBy("nise").orderBy("fecha").rowsBetween(-30, -1)
    )

    ref_nise = (
        dias_nise.withColumn("dias_previos", F.count(F.lit(1)).over(ventana_ref))
        .withColumn(
            "p_average_ref_30d_nise",
            F.avg("p_average_dia_hist").over(ventana_ref),
        )
        # Historial minimo para un nivel de referencia estable
        .filter(F.col("dias_previos") >= 20)
        .select("nise", "fecha", "p_average_ref_30d_nise")
    )

    casos = (
        lecturas.join(
            curvas, ["tipo_curva", "tipo_dia", "epoca", "bloque"], "inner"
        )
        .groupBy(
            "nise", "tipo_curva", "etiqueta_curva", "fecha", "tipo_dia", "epoca"
        )
        .agg(
            F.count(F.lit(1)).alias("bloques_dia"),
            F.avg("demanda").alias("p_average_dia_kw"),
            F.sum("demanda").alias("demanda_real_total_dia_kw"),
            # SUM(valor_pu) x p_average = demanda perfilada del dia
            # (valor_pu, NUNCA valor_kw: bug de doble escalado FASE 1)
            F.sum("valor_pu").alias("suma_valor_pu"),
            F.first("metodologia_ganadora").alias("metodologia_ganadora"),
            F.first("nises_grupo").alias("nises_grupo"),
        )
        .join(ref_nise, ["nise", "fecha"], "inner")
    )

    return (
        casos.withColumn(
            "demanda_perfilada_total_dia_kw",
            F.col("p_average_ref_30d_nise") * F.col("suma_valor_pu"),
        )
        .withColumn(
            "factor_gspgcf_nise",
            F.col("demanda_real_total_dia_kw")
            / F.col("demanda_perfilada_total_dia_kw"),
        )
        # Completitud >= 95% y perfilada > 0 (excluye valor_pu NULL de
        # grupos degenerados RANGO DE 0-20 con Q3=0)
        .filter(
            (F.col("bloques_dia") >= BLOQUES_MINIMOS)
            & (F.col("demanda_perfilada_total_dia_kw") > 0)
        )
        .select(
            "nise",
            "tipo_curva",
            "etiqueta_curva",
            "fecha",
            "tipo_dia",
            "epoca",
            F.round("factor_gspgcf_nise", 6).alias("factor_gspgcf_nise"),
            F.round("p_average_ref_30d_nise", 6).alias("p_average_ref_30d_nise"),
            F.round("p_average_dia_kw", 6).alias("p_average_dia_kw"),
            F.round("demanda_real_total_dia_kw", 4).alias(
                "demanda_real_total_dia_kw"
            ),
            F.round("demanda_perfilada_total_dia_kw", 4).alias(
                "demanda_perfilada_total_dia_kw"
            ),
            "bloques_dia",
            "metodologia_ganadora",
            "nises_grupo",
        )
    )


@dp.materialized_view(
    name="factores_correccion_celda_fallback",
    comment=(
        "Tabla C: factor de correccion GSPGCF de fallback por celda "
        "(tipo_curva x tipo_dia x epoca), para NISEs sin historial "
        "suficiente (<30 dias) o NISEs nuevos. factor_mediana_celda = "
        "mediana de los factores por NISE-dia de la celda (sobre TODA la "
        "ventana feb-jul 2026); p_average_celda_kw = nivel promedio de "
        "demanda de la celda (promedio por NISE primero, para no sesgar "
        "por cantidad de dias); nises_celda = NISEs con factor en la "
        "celda. Extiende/absorbe factores_correccion_historicos (FASE "
        "3.2). El pipeline consumidor lo usa como segunda fuente (clip "
        "0.5-2.0 con auditoria fuente_factor)."
    ),
)
def factores_correccion_celda_fallback():
    factores = spark.read.table("factores_correccion_nise_historicos")

    factor_celda = factores.groupBy("tipo_curva", "tipo_dia", "epoca").agg(
        F.percentile_approx("factor_gspgcf_nise", 0.5).alias(
            "factor_mediana_celda"
        ),
        F.stddev("factor_gspgcf_nise").alias("factor_std_celda"),
        F.count(F.lit(1)).alias("dias_con_factor"),
        F.countDistinct("nise").alias("nises_celda"),
        F.first("etiqueta_curva").alias("etiqueta_curva"),
        F.first("metodologia_ganadora").alias("metodologia_ganadora"),
    )

    # Nivel de la celda: promedio por NISE primero, luego promedio de
    # la celda (sin sesgo por cantidad de dias de cada NISE).
    nivel_nise = factores.groupBy(
        "tipo_curva", "tipo_dia", "epoca", "nise"
    ).agg(F.avg("p_average_dia_kw").alias("p_average_nise_kw"))

    nivel_celda = nivel_nise.groupBy("tipo_curva", "tipo_dia", "epoca").agg(
        F.avg("p_average_nise_kw").alias("p_average_celda_kw")
    )

    return (
        factor_celda
        .join(nivel_celda, ["tipo_curva", "tipo_dia", "epoca"], "inner")
        .select(
            "tipo_curva",
            "etiqueta_curva",
            "tipo_dia",
            "epoca",
            F.round("factor_mediana_celda", 6).alias("factor_mediana_celda"),
            F.round("factor_std_celda", 6).alias("factor_std_celda"),
            F.round("p_average_celda_kw", 6).alias("p_average_celda_kw"),
            "nises_celda",
            "dias_con_factor",
            "metodologia_ganadora",
        )
    )


@dp.materialized_view(
    name="factores_correccion_celda_fecha",
    comment=(
        "Factor de correccion GSPGCF promedio por celda x fecha "
        "(tipo_curva x tipo_dia x epoca x fecha), calculado sobre TODOS "
        "los NISE-dias con factor propio (el ad-hoc de FASE 3.2 usaba 1 "
        "dia determinista por NISE). Absorbe y reemplaza "
        "factores_correccion_historicos (que queda como referencia "
        "historica). factor_promedio = promedio de "
        "factor_gspgcf_nise de los NISEs de la celda ese dia; casos = "
        "NISE-dias considerados. Evolucion temporal del factor de celda "
        "dentro de la ventana feb-jul 2026."
    ),
)
def factores_correccion_celda_fecha():
    return (
        spark.read.table("factores_correccion_nise_historicos")
        .groupBy("tipo_curva", "tipo_dia", "epoca", "fecha")
        .agg(
            F.round(F.avg("factor_gspgcf_nise"), 6).alias("factor_promedio"),
            F.round(F.stddev("factor_gspgcf_nise"), 6).alias("factor_std"),
            F.count(F.lit(1)).alias("casos"),
            F.first("etiqueta_curva").alias("etiqueta_curva"),
            F.first("metodologia_ganadora").alias("metodologia_ganadora"),
        )
    )


@dp.materialized_view(
    name="validacion_factores_nise",
    comment=(
        "Vista de validacion del factor GSPGCF por NISE: por nise x "
        "tipo_dia x epoca trae dias_con_factor, factor_mediana_nise, "
        "clasificacion (TIPICO si |factor_mediana - 1| <= 0.5, ATIPICO "
        "si no) y fuente_factor_recomendada (NISE_PROPIO si >=30 dias "
        "con factor, CELDA_FALLBACK si no). Usar para auditar la "
        "distribucion tipico/atipico y la cobertura de NISEs con factor "
        "propio vs fallback antes de aplicar factores en produccion."
    ),
)
def validacion_factores_nise():
    factores = spark.read.table("factores_correccion_nise_historicos")

    return (
        factores.groupBy(
            "nise", "tipo_curva", "etiqueta_curva", "tipo_dia", "epoca"
        )
        .agg(
            F.count(F.lit(1)).alias("dias_con_factor"),
            F.percentile_approx("factor_gspgcf_nise", 0.5).alias(
                "factor_mediana_nise"
            ),
            F.min("fecha").alias("primer_fecha"),
            F.max("fecha").alias("ultima_fecha"),
        )
        .select(
            "nise",
            "tipo_curva",
            "etiqueta_curva",
            "tipo_dia",
            "epoca",
            "dias_con_factor",
            F.round("factor_mediana_nise", 6).alias("factor_mediana_nise"),
            "primer_fecha",
            "ultima_fecha",
            F.when(
                F.abs(F.col("factor_mediana_nise") - F.lit(1.0)) <= F.lit(0.5),
                F.lit("TIPICO"),
            )
            .otherwise(F.lit("ATIPICO"))
            .alias("clasificacion_nise"),
            F.when(
                F.col("dias_con_factor") >= 30, F.lit("NISE_PROPIO")
            )
            .otherwise(F.lit("CELDA_FALLBACK"))
            .alias("fuente_factor_recomendada"),
        )
    )
