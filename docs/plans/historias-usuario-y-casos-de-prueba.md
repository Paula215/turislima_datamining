# Historias de Usuario y Casos de Prueba

## Contexto
Este documento resume las historias de usuario (HU) que habilitaron el desarrollo del pipeline cultural y los casos de prueba asociados.

Alcance funcional actual:
- Scraping multi fuente (BNP, MALI, Joinnus, Places)
- Normalizacion a esquema canonico unico
- Generacion y validacion de embeddings
- Publicacion opcional en MongoDB (web y reco)
- Ejecucion automatizada por scheduler y GitHub Actions

## Historias de Usuario

### HU-01 - Ingesta multi fuente
Como analista de datos, quiero extraer eventos y lugares desde fuentes heterogeneas para construir un catalogo unico de actividades.

Criterios de aceptacion:
- El pipeline ejecuta fuentes configuradas por parametro `--sources`.
- El pipeline usa todas las fuentes cuando no se pasa `--sources`.
- Una falla en una fuente no rompe toda la corrida.

### HU-02 - Normalizacion canonica
Como consumidor de datos, quiero un esquema estandar para consultar eventos y lugares con campos consistentes.

Criterios de aceptacion:
- El output contiene columnas obligatorias (`entity_id`, `entity_type`, `titulo`, `fuente`, `texto_embedding`, `url_origen`).
- No existen duplicados por `entity_id`.
- Se distinguen entidades por `entity_type` (`event` y `place`).

### HU-03 - Persistencia y trazabilidad de corridas
Como operador, quiero persistir outputs y metricas por corrida para auditar calidad y cobertura.

Criterios de aceptacion:
- Se generan `eventos_estandar.csv` y `eventos_estandar.json`.
- Se genera snapshot historico por `run_id`.
- Se genera `stats_<run_id>.json` con conteos por fuente, tipo y precio.

### HU-04 - Embeddings para busqueda semantica
Como modulo de recomendacion, quiero embeddings consistentes para habilitar recuperacion semantica de eventos y lugares.

Criterios de aceptacion:
- Se generan `vectors_<run_id>.npy`, `metadata_<run_id>.json` y `contract_<run_id>.json`.
- La dimension de embedding respeta el contrato (`EMBEDDING_DIM`).
- La corrida continua aunque FAISS no este instalado.

### HU-05 - Calidad automatica del output canonico
Como responsable de calidad de datos, quiero validaciones automaticas para detectar regresiones de cobertura y estructura.

Criterios de aceptacion:
- `validar_output.py` falla si faltan columnas requeridas o hay duplicados.
- Se emite reporte JSON con estado `PASS`, `WARN` o `FAIL`.
- Se compara contra corrida previa para detectar caidas por fuente.

### HU-06 - Calidad automatica de embeddings
Como responsable de calidad de recomendacion, quiero verificar integridad estructural y senales semanticas de embeddings.

Criterios de aceptacion:
- `validate_embeddings.py` valida shape, alineacion metadata-vectores y valores finitos.
- Se genera `quality_<run_id>.json` y `quality_latest.json`.
- Se reporta `self_recall@k` y near-duplicates.

### HU-07 - Publicacion web en MongoDB
Como backend web, quiero upsert de entidades para exponer catalogo actualizado sin duplicados.

Criterios de aceptacion:
- Con `--write-mongo-web` se realiza upsert por `_id=entity_id`.
- Se actualiza `last_seen_run_id` y estado de actividad.
- En corrida parcial o dry-run no se aplica limpieza destructiva.

### HU-08 - Publicacion reco en MongoDB
Como backend de recomendacion, quiero persistir vectores y metadata minima para consultas semanticas filtrables.

Criterios de aceptacion:
- Con `--write-mongo-reco` se realiza upsert de embeddings por `_id=entity_id`.
- Se almacenan campos de filtro (`tipo`, `fecha_inicio`, `precio`, `fuente`, `ciudad`, `url_origen`).
- Si embeddings y filas no alinean, el pipeline reporta error controlado.

### HU-09 - Limpieza segura de stale docs
Como operador, quiero controlar inactivacion/eliminacion de documentos stale sin perder datos en corridas parciales.

Criterios de aceptacion:
- Solo en corridas completas se evalua limpieza stale.
- `MONGO_HARD_DELETE_STALE` controla hard delete vs inactivacion.
- `MONGO_DELETE_AFTER_MISSED_FULL_RUNS` define ventana de seguridad.

### HU-10 - Ejecucion automatizada en CI
Como equipo de datos, quiero una ejecucion semanal automatica y un disparador manual parametrizable.

Criterios de aceptacion:
- El workflow ejecuta schedule semanal y `workflow_dispatch`.
- El workflow permite parametros de fuentes, embeddings y publicacion Mongo.
- El workflow sube artifacts de outputs y logs.

### HU-11 - Seguridad de secretos
Como equipo de desarrollo, quiero separar secretos de codigo para evitar exposicion de credenciales.

Criterios de aceptacion:
- `.env` local no se versiona.
- Existen plantillas en `.env.example`.
- CI usa `secrets` y `vars` para configuracion sensible/no sensible.

### HU-12 - Operacion funcional minima
Como operador, quiero un script funcional de punta a punta para correr y validar rapidamente una corrida.

Criterios de aceptacion:
- `scripts/run_funcional.sh` ejecuta pipeline con variables de entorno.
- El script invoca `validar_output.py` al finalizar.
- El script soporta activar/desactivar embeddings y Mongo por flags.

### HU-13 - Contrato canónico estable para recomendación
Como backend de recomendación, quiero identificadores estables por entidad para no invalidar historial de interacciones entre corridas.

Criterios de aceptacion:
- El output canónico incluye `poi_id` y `poi_id_version`.
- El output canónico incluye `categoria_normalizada`, `geo_hash` y `fecha_run`.
- `poi_id` es determinístico para entidades equivalentes en corridas consecutivas con mismo input.

## Casos de Prueba

Formato:
- Tipo: F = funcional, I = integracion, N = negativo

### CP-01 (HU-01, F) - Corrida completa por defecto
Precondiciones:
- Entorno Python activo
- Dependencias instaladas

Pasos:
1. Ejecutar `python pipeline/pipeline.py`.

Resultado esperado:
- El log muestra fuentes `bnp`, `mali`, `joinnus`, `places`.
- Se genera output canonico con `total_eventos > 0`.

### CP-02 (HU-01, F) - Corrida parcial por fuentes
Precondiciones:
- Entorno activo

Pasos:
1. Ejecutar `python pipeline/pipeline.py --sources bnp joinnus`.

Resultado esperado:
- El log solo procesa BNP y Joinnus.
- El output contiene fuentes esperadas para la corrida parcial.

### CP-03 (HU-01, N) - Falla controlada de fuente
Precondiciones:
- Simular fallo en una fuente (por ejemplo token invalido o timeout)

Pasos:
1. Ejecutar pipeline completo.

Resultado esperado:
- Se registra error de fuente.
- El pipeline continua con fuentes restantes.

### CP-04 (HU-02, F) - Columnas requeridas en output
Precondiciones:
- Existe `output/eventos_estandar.csv`

Pasos:
1. Ejecutar `python scripts/validar_output.py --expected-sources bnp mali joinnus places --emit-json`.

Resultado esperado:
- No falla por columnas faltantes.
- Se genera reporte JSON de calidad.

### CP-05 (HU-02, N) - Duplicados por entity_id
Precondiciones:
- Forzar duplicado en CSV de prueba

Pasos:
1. Ejecutar `validar_output.py` sobre dataset alterado.

Resultado esperado:
- Estado `FAIL` por duplicados.

### CP-06 (HU-03, F) - Artefactos de corrida
Precondiciones:
- Corrida ejecutada

Pasos:
1. Revisar `output/` y `logs/`.

Resultado esperado:
- Existen CSV, JSON, snapshot y stats del `run_id`.

### CP-07 (HU-04, F) - Generacion de embeddings
Precondiciones:
- `--skip-embeddings` desactivado

Pasos:
1. Ejecutar pipeline completo.

Resultado esperado:
- Se crean archivos `vectors_`, `metadata_`, `contract_`.
- Se reporta shape `(N, 384)`.

### CP-08 (HU-04, N) - Dimension inesperada
Precondiciones:
- Configurar `EMBEDDING_DIM` incompatible

Pasos:
1. Ejecutar pipeline con embeddings.

Resultado esperado:
- Se reporta error de dimension inesperada.

### CP-09 (HU-05, F) - Semaforo de calidad canonica
Precondiciones:
- Corrida reciente disponible

Pasos:
1. Ejecutar `validar_output.py --emit-json`.

Resultado esperado:
- Se obtiene estado `PASS` o `WARN` segun datos.

### CP-10 (HU-05, F) - Deteccion de caida de cobertura
Precondiciones:
- Existen al menos dos `stats_*.json`

Pasos:
1. Ejecutar `validar_output.py`.

Resultado esperado:
- Se reportan `warnings` cuando la caida supera umbral.

### CP-11 (HU-06, F) - Validacion de embeddings latest
Precondiciones:
- Existen archivos latest de embeddings

Pasos:
1. Ejecutar `python scripts/validate_embeddings.py --run-id latest`.

Resultado esperado:
- Se genera reporte de calidad.
- Se actualiza `quality_latest.json`.

### CP-12 (HU-06, N) - Metadata no alineada
Precondiciones:
- Alterar metadata para tener distinta longitud que vectores

Pasos:
1. Ejecutar `validate_embeddings.py`.

Resultado esperado:
- Estado `FAIL` por desalineacion.

### CP-13 (HU-07, I) - Upsert Mongo web
Precondiciones:
- `MONGO_URI_WEB` y DB/COLL configurados

Pasos:
1. Ejecutar `python pipeline/pipeline.py --write-mongo-web --skip-embeddings`.

Resultado esperado:
- Log de `Web upsert` con contadores.

### CP-14 (HU-08, I) - Upsert Mongo reco
Precondiciones:
- `MONGO_URI_RECO` y DB/COLL configurados

Pasos:
1. Ejecutar `python pipeline/pipeline.py --write-mongo-reco`.

Resultado esperado:
- Log de `Reco upsert` con contadores.

### CP-15 (HU-09, F) - Limpieza omitida en corrida parcial
Precondiciones:
- Flags Mongo activadas

Pasos:
1. Ejecutar corrida parcial con `--sources bnp`.

Resultado esperado:
- Log indica limpieza omitida por corrida parcial.

### CP-16 (HU-09, F) - Limpieza en corrida completa
Precondiciones:
- Flags de limpieza configuradas

Pasos:
1. Ejecutar corrida completa con Mongo activo.

Resultado esperado:
- Se ejecuta inactivacion o hard delete segun variables.

### CP-17 (HU-10, I) - Workflow manual parametrizado
Precondiciones:
- Workflow habilitado en GitHub

Pasos:
1. Disparar `workflow_dispatch` con `skip_embeddings=true`.

Resultado esperado:
- Pipeline se ejecuta sin generar embeddings.
- Se publican artifacts de output y logs.

### CP-18 (HU-11, F) - Politica de secretos
Precondiciones:
- Repo local

Pasos:
1. Ejecutar `git status`.
2. Verificar que `.env` no aparece como tracked.

Resultado esperado:
- `.env` permanece fuera de versionado.

### CP-19 (HU-12, F) - Script funcional
Precondiciones:
- Entorno activo

Pasos:
1. Ejecutar `PYTHON_BIN=python bash scripts/run_funcional.sh`.

Resultado esperado:
- El script ejecuta pipeline y valida output al final.

### CP-20 (HU-13, F) - Campos DM-2 presentes y consistentes
Precondiciones:
- Corrida completa reciente disponible

Pasos:
1. Revisar `output/eventos_estandar.csv`.
2. Verificar columnas: `poi_id`, `poi_id_version`, `categoria_normalizada`, `geo_hash`, `fecha_run`.

Resultado esperado:
- `poi_id` sin nulos y único por fila.
- `categoria_normalizada` y `fecha_run` sin nulos.
- `geo_hash` presente para entidades con lat/lng.

### CP-21 (HU-13, F) - Estabilidad determinística de poi_id
Precondiciones:
- CSVs raw del mismo run disponibles en `output/raw/`.

Pasos:
1. Cargar raw `bnp/mali/joinnus/places` de un mismo `run_id`.
2. Ejecutar `normalize_all` dos veces sobre el mismo input.
3. Comparar `poi_id` por `entity_id` entre ambas salidas.

Resultado esperado:
- `drift_count = 0` para `poi_id`.

## Matriz de trazabilidad (resumen)
- HU-01: CP-01, CP-02, CP-03
- HU-02: CP-04, CP-05
- HU-03: CP-06
- HU-04: CP-07, CP-08
- HU-05: CP-09, CP-10
- HU-06: CP-11, CP-12
- HU-07: CP-13
- HU-08: CP-14
- HU-09: CP-15, CP-16
- HU-10: CP-17
- HU-11: CP-18
- HU-12: CP-19
- HU-13: CP-20, CP-21
