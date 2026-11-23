from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)
import struct
from functools import reduce

"""
Conversión DBF -> tablas UC de los archivos oficiales SIGEL / INFOCLIENTES
en /Volumes/workspace/alunizaje/datos_oficiales/infoclientes/.

Los 7 .dbf (dBASE III) se parsean con lector binario propio porque Spark no
tiene lector nativo del formato; el parseo corre en el driver y los registros
se materializan por chunks (~50k filas) en DataFrames distribuidos desde su
creación (spark.createDataFrame con esquema explícito + unionByName).

Tablas producidas (en {catalog}.{schema}, grano = un registro DBF):
  1. sigel_circuit_source        136 registros. Catálogo de fuentes de
     circuito del SIGEL: ID/NAME (p.ej. '7201 PH BELEN'), SUBSTATION,
     CODIGOSUBE, CODIGOCIRC. Referencia para unir alimentadores de las
     capas GIS (IDALIMENTA) con circuitos.
  2. medicion_media_tension      274 registros. Medidores de media tensión:
     NUMEROMEDI, IDALIMENTA (alimentador, p.ej. '1104 PINARES'),
     RELACIONTC/RELACIONTP, NUM_TRAFOT/NUM_TRAF_1..5 (trafos), CIRCUITSOU.
  3. medicion_baja_tension_c20   3.872 registros. Medidores BT C20:
     NUMEROMEDI, IDALIMENTA, NOMBRECAPA ('Banco Transformador'),
     CONEXIONSE, CAPACIDAD_.
  4. servicio_baja_tension      643.258 registros. Servicios BT: NUMEROMEDI
     + IDALIMENTA (p.ej. '3001 CORON-S.BLAS') — la llave medidor→alimentador
     a escala de toda la población BT; TIPOMEDICI, CONEXION, N_TRANSF_1..3.
  5. puesto_luminaria            99.682 registros. Puestos de alumbrado:
     ALIMENTADO (alimentador), POTENCIA_W, TIPOLUZ, MONT_LUM.
  6. sigel_infoclientes         647.831 registros, 75 campos. Catálogo
     comercial SIGEL (NUMEROMEDI, NISE, TIPOCURVA, TARIFA, DEMANDA,
     ubicación, mediciones del periodo). Snapshot distinto de
     INFOCLIENTES_ArcGIS; ambos se conservan para trazabilidad.
  7. infoclientes_arcgis_oficial 647.161 registros, 75 campos. Mismo
     formato que workspace.project_base.bronze_infoclientes_arcgis (que ya
     existe con nombres renombrados); esta copia conserva los nombres
     originales DBF (mayúsculas) como respaldo fiel del archivo oficial.

Notas de tipado y cruces:
  - N/F con decimales -> double; N/F enteros -> long; C/D/L -> string
    (fechas quedan YYYYMMDD para no perder fidelidad; castear al consumir).
  - NUMEROMEDI es el número de medidor: en plata_950_all la columna
    `medidor` es string del mismo número (unir con CAST o lpad si aplica).
  - NISE aquí es numérico (long); en el schema alunizaje es string.
  - Registros marcados como borrados ('*' en dBASE) se omiten.
  - El path raíz es configurable: project.path_infoclientes_dbf en el yml
    del pipeline (default = volumen datos_oficiales).
"""

DBF_DIR = spark.conf.get(
    "project.path_infoclientes_dbf",
    "/Volumes/workspace/alunizaje/datos_oficiales/infoclientes",
)


# --------------------------- Parser dBASE III ---------------------------


def _dbf_meta(path):
    """Cabecera dBASE III: (campos, registros declarados, offset datos, largo registro)."""
    with open(path, "rb") as f:
        h = f.read(32)
        n_records, = struct.unpack("<I", h[4:8])
        header_len, = struct.unpack("<H", h[8:10])
        rec_len, = struct.unpack("<H", h[10:12])
        fields = []
        f.seek(32)
        while True:
            fd = f.read(32)
            if not fd or fd[0:1] == b"\r":
                break
            name = fd[0:11].split(b"\x00")[0].decode("latin-1").strip()
            if name:
                fields.append((name, chr(fd[11]), fd[16], fd[17]))
    return fields, n_records, header_len, rec_len


def _spark_type(ftype, fdec):
    if ftype in ("N", "F"):
        return DoubleType() if fdec > 0 else LongType()
    return StringType()


def _cast(raw, ftype, fdec):
    raw = raw.strip()
    if not raw:
        return None
    if ftype in ("N", "F"):
        try:
            return float(raw) if fdec > 0 else int(raw)
        except ValueError:
            return None
    return raw


def _iter_rows(path, fields, header_len, rec_len):
    with open(path, "rb") as f:
        f.seek(header_len)
        while True:
            rec = f.read(rec_len)
            if len(rec) < rec_len:
                return
            if rec[0:1] != b" ":  # salta registros marcados como borrados
                continue
            row, pos = [], 1
            for _name, ftype, flen, fdec in fields:
                raw = rec[pos:pos + flen].decode("latin-1", "replace")
                row.append(_cast(raw, ftype, fdec))
                pos += flen
            yield tuple(row)


def dbf_dataframe(path, chunk_rows=50_000):
    """Convierte un .dbf en un DataFrame Spark tipado (chunks en el driver)."""
    fields, _n_records, header_len, rec_len = _dbf_meta(path)
    schema = StructType(
        [
            StructField(name, _spark_type(ftype, fdec), True)
            for name, ftype, _flen, fdec in fields
        ]
    )
    chunks, buf = [], []
    for row in _iter_rows(path, fields, header_len, rec_len):
        buf.append(row)
        if len(buf) >= chunk_rows:
            chunks.append(spark.createDataFrame(buf, schema))
            buf = []
    if buf:
        chunks.append(spark.createDataFrame(buf, schema))
    if not chunks:
        return spark.createDataFrame([], schema)
    return reduce(lambda a, b: a.unionByName(b), chunks)


# ------------------------------- Tablas ---------------------------------


@dp.materialized_view(
    name="sigel_circuit_source",
    comment=(
        "Catalogo de fuentes de circuito del SIGEL (136 registros, grano = "
        "circuito). Campos clave: ID/NAME (identificador del circuito, p.ej. "
        "'7201 PH BELEN'), SUBSTATION, CODIGOSUBE, CODIGOCIRC. Referencia "
        "para unir los alimentadores de las capas GIS (IDALIMENTA de "
        "medicion_media_tension, medicion_baja_tension_c20 y "
        "servicio_baja_tension) con circuitos y subestaciones, y con las "
        "cabeceras PME. Nombres de columnas originales del DBF."
    ),
)
def sigel_circuit_source():
    return dbf_dataframe(f"{DBF_DIR}/SIGEL_CircuitSource.dbf")


@dp.materialized_view(
    name="medicion_media_tension",
    comment=(
        "Medidores de media tension del GIS (274 registros, grano = medidor "
        "MT). Campos clave: NUMEROMEDI (numero de medidor), IDALIMENTA "
        "(alimentador, p.ej. '1104 PINARES'), RELACIONTC/RELACIONTP, "
        "NUM_TRAFOT y NUM_TRAF_1..5 (transformadores asociados), CIRCUITSOU. "
        "Une el medidor con su alimentador/trafo: insumo para vincular "
        "clientes MT con cabeceras de circuito. Nombres de columnas "
        "originales del DBF."
    ),
)
def medicion_media_tension():
    return dbf_dataframe(f"{DBF_DIR}/Medicion_Media_Tension.dbf")


@dp.materialized_view(
    name="medicion_baja_tension_c20",
    comment=(
        "Medidores de baja tension C20 del GIS (3.872 registros, grano = "
        "medidor BT C20). Campos clave: NUMEROMEDI, IDALIMENTA, "
        "NOMBRECAPA ('Banco Transformador'), CONEXIONSE, CAPACIDAD_. "
        "Nombres de columnas originales del DBF."
    ),
)
def medicion_baja_tension_c20():
    return dbf_dataframe(f"{DBF_DIR}/Medicion__Baja_Tension_C20.dbf")


@dp.materialized_view(
    name="servicio_baja_tension",
    comment=(
        "Servicios de baja tension del GIS (643.258 registros, grano = "
        "servicio). LA llave medidor->alimentador a escala de toda la "
        "poblacion BT: NUMEROMEDI + IDALIMENTA (p.ej. '3001 CORON-S.BLAS'). "
        "Ademas TIPOMEDICI, CONEXION, N_TRANSF_1..3, CAPACIDAD_. Crucelo "
        "con SIGEL_INFOCLIENTES por NUMEROMEDI para llevar el NISE al "
        "alimentador/circuito, y con sigel_circuit_source para el nombre del "
        "circuito. Nombres de columnas originales del DBF."
    ),
)
def servicio_baja_tension():
    return dbf_dataframe(f"{DBF_DIR}/Servicio_Baja_Tension.dbf")


@dp.materialized_view(
    name="puesto_luminaria",
    comment=(
        "Puestos de alumbrado publico del GIS (99.682 registros, grano = "
        "puesto/luminaria). Campos clave: ALIMENTADO (alimentador), "
        "POTENCIA_W, TIPOLUZ (LED...), MONT_LUM, CANTIDADLA. Util para "
        "descontar carga de alumbrado de las cabeceras PME. Nombres de "
        "columnas originales del DBF."
    ),
)
def puesto_luminaria():
    return dbf_dataframe(f"{DBF_DIR}/PuestoLuminaria.dbf")


@dp.materialized_view(
    name="sigel_infoclientes",
    comment=(
        "Catalogo comercial SIGEL (647.831 registros, 75 campos, grano = "
        "medidor). NUMEROMEDI, NISE, TIPOCURVA, TARIFA, DEMANDA, ubicacion "
        "(provincia/canton/distrito), mediciones del periodo. Snapshot "
        "distinto de INFOCLIENTES_ArcGIS; ambos se conservan para "
        "trazabilidad y comparacion. NISE es numerico (long): castear a "
        "string para unir con las tablas del schema alunizaje. Nombres de "
        "columnas originales del DBF."
    ),
)
def sigel_infoclientes():
    return dbf_dataframe(f"{DBF_DIR}/SIGEL_INFOCLIENTES.dbf")


@dp.materialized_view(
    name="infoclientes_arcgis_oficial",
    comment=(
        "Copia fiel del catalogo INFOCLIENTES_ArcGIS (647.161 registros, 75 "
        "campos, grano = medidor) con los nombres de columna ORIGINALES del "
        "DBF en mayusculas. La version renombrada y tipada ya existe como "
        "workspace.project_base.bronze_infoclientes_arcgis; esta copia es "
        "respaldo del archivo oficial y permite auditoria campo a campo "
        "contra el bronze."
    ),
)
def infoclientes_arcgis_oficial():
    return dbf_dataframe(f"{DBF_DIR}/INFOCLIENTES_ArcGIS.dbf")
