-- Embudo de verificacion del pipeline alunizaje_curvas_carga
--
-- Esperado (muestra 50 por categoria = todo el catalogo si hay menos, validado 14-11-2026):
--   5935 NISEs en muestra -> 5301 con completitud >= 95% -> 5212 en plata_950_all
--   100 de 203 categorias con >=30 curvas; 3 con 30+ disponibles quedaron en 26-28
--   (antes: muestra 2/cat 397->347->342; muestra 5/cat 918->813->804)
--
-- Nota: workspace.alunizaje corresponde a catalog=${var.catalog} y
-- schema=${var.schema_alunizaje}; ajusta los nombres si desplegaste
-- con otro catalogo.

WITH muestra AS (
  SELECT nise, etiqueta_curva
  FROM workspace.alunizaje.muestra_clientes_curvas
),
comp_ok AS (
  SELECT DISTINCT nise
  FROM workspace.alunizaje.plata_carga_completitud_all
  WHERE flag_completitud_general_95 = 1
),
nises_950 AS (
  SELECT DISTINCT nise
  FROM workspace.alunizaje.plata_950_all
)
SELECT
  m.etiqueta_curva,
  count(DISTINCT m.nise) AS nises_muestra,
  count(DISTINCT c.nise) AS nises_comp_ok,
  count(DISTINCT q.nise) AS nises_950
FROM muestra m
LEFT JOIN comp_ok c ON m.nise = c.nise
LEFT JOIN nises_950 q ON m.nise = q.nise
GROUP BY m.etiqueta_curva
ORDER BY m.etiqueta_curva;

SELECT
  (SELECT count(DISTINCT nise) FROM workspace.alunizaje.muestra_clientes_curvas) AS total_muestra,
  (SELECT count(DISTINCT nise) FROM workspace.alunizaje.plata_carga_completitud_all
     WHERE flag_completitud_general_95 = 1) AS total_comp_ok,
  (SELECT count(DISTINCT nise) FROM workspace.alunizaje.plata_950_all) AS total_950;
