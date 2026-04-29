# Medallion cutover plan

> Owner: data-mining team · Status: draft · Last updated: 2026-04-28

Plan operativo para migrar la app web y el recomendador del stack legacy
(MongoDB Atlas + FAISS local) al stack medallón en Azure (ADLS Gen2 +
Cosmos DB for MongoDB vCore con vector search nativo).

---

## Estado actual

| Capa | Backend legacy | Backend medallón |
|---|---|---|
| Catálogo web | MongoDB Atlas (`MONGO_URI_WEB`) | Cosmos vCore `catalog.eventos` |
| Recomendador (vectores) | FAISS local + MongoDB Atlas (`MONGO_URI_RECO`) | Cosmos vCore `reco.eventos_vectors` (HNSW `cosmosSearch`) |
| Storage de runs | `output/`, `embeddings/` (filesystem) | ADLS Gen2 (`bronze/silver/gold`) |
| Compute | GitHub Actions self-hosted runner | Azure Container Apps Job (semanal) |

El pipeline ya escribe a **ambos** backends en paralelo cuando los flags
correspondientes están activos. La elección entre Atlas y Cosmos para el
**lado escritor** se hace con la env var `RECO_BACKEND` (default `atlas`).

El **lado lector** (la app web + el recomendador) debe introducir el mismo
flag para poder leer del backend que toque durante cada fase del cutover.

---

## Flag `RECO_BACKEND`

| Valor | Semántica writer (gold stage) | Semántica reader (app/recomendador) |
|---|---|---|
| `atlas` *(default)* | Upsert a Mongo Atlas (`MONGO_URI_*`). FAISS local sigue construyéndose. | Lee de Atlas; queries semánticas usan FAISS. |
| `cosmos` | Upsert a Cosmos vCore (`COSMOS_URI` o Key Vault `cosmos-uri`). HNSW se asegura idempotente. | Lee de Cosmos; queries semánticas usan `$search.cosmosSearch`. |
| `dual` *(propuesto)* | Escribe en ambos. | Lee de Cosmos primario; si falla, fallback a Atlas. |

> El modo `dual` es opcional para la app — simplifica el rollback. La
> implementación del lado lector queda en el repo de la app, no aquí.
> Aquí el writer ya escribe a Atlas y, si `RECO_BACKEND=cosmos`, también
> a Cosmos. Es **trivial extender el dispatcher para `dual`** si la app lo
> requiere (escribir a los dos en cada run).

---

## Fases

### Fase 0 — Pre-flight (esta fase)

Pre-requisitos para iniciar:

- [x] `pipeline/cosmos_sink.py` con API espejo de `mongo_sink.py` (BD-7)
- [x] HNSW `cosmosSearch` index idempotente (BD-8)
- [x] `cultural_pipeline/scripts/azure_provision.sh` listo (BD-11)
- [x] Dockerfile + workflow ACR build/push (BD-10)
- [x] Validadores medallón Bronze/Silver/Gold (BD-9)
- [ ] Recursos Azure provisionados (`./scripts/azure_provision.sh`)
- [ ] Imagen publicada en ACR (`az acr build` o GitHub Actions)
- [ ] Container Apps Job corriendo al menos una vez con `RECO_BACKEND=atlas` (sanity)

### Fase 1 — Doble escritura (sombra)

**Objetivo**: durante 1–2 corridas semanales, escribir a Cosmos en sombra
sin que la app lo lea aún.

1. Configurar el Container Apps Job:
   ```bash
   az containerapp job update -n cultural-pipeline-job -g <RG> \
     --set-env-vars RECO_BACKEND=cosmos \
                    LAKE_BACKEND=azure \
                    COSMOS_DB_CATALOG=catalog \
                    COSMOS_DB_RECO=reco
   ```
2. Disparar manualmente una corrida full:
   ```bash
   az containerapp job start -n cultural-pipeline-job -g <RG>
   ```
3. Verificar que Cosmos recibió docs:
   ```bash
   mongo "$COSMOS_URI" --eval "db.getSiblingDB('reco').eventos_vectors.countDocuments({})"
   ```
4. Correr el script de paridad (ver `scripts/compare_reco_backends.py`):
   ```bash
   python cultural_pipeline/scripts/compare_reco_backends.py \
     --queries cultural_pipeline/scripts/cutover_queries.json \
     --top-k 10 --emit-json
   ```
5. **Criterio de salida**: Jaccard@10 ≥ 0.7 promedio sobre las queries
   representativas. Si menor, investigar (puede ser drift entre el modelo
   de embeddings, el índice HNSW recién creado vs FAISS exacto, o
   diferencias en la cobertura de eventos).

### Fase 2 — Lectura primaria desde Cosmos

**Objetivo**: la app lee de Cosmos; Atlas + FAISS quedan en standby.

1. App: deploy con `RECO_BACKEND=cosmos`. Atlas client config se mantiene
   pero solo se usa como fallback explícito (modo `dual` en el reader).
2. Recomendador: cambia `search_events()` para usar
   `cosmos_sink.search_similar()` en lugar de FAISS.
3. Mantener la doble escritura del writer durante 2 semanas más para
   permitir rollback.
4. Monitorear:
   - Latencia p95 de queries semánticas (Cosmos vCore vs FAISS local).
   - Tasa de error en `$search.cosmosSearch`.
   - Diferencias en CTR (si hay telemetría) entre semanas pre/post cutover.

### Fase 3 — Decomisión Atlas + FAISS (BD-13)

Solo si Fase 2 lleva ≥ 2 semanas estable:

1. Quitar la doble escritura: `RECO_BACKEND=cosmos` permanece, eliminar
   secrets `MONGO_URI_WEB` / `MONGO_URI_RECO` del job y de Key Vault.
2. Pausar el cluster Atlas (no eliminar — backup de seguridad por 30 días).
3. Archivar `embeddings/faiss_*.index` y la lógica de
   `pipeline/build_faiss_index.py` (mantener como utilidad opt-in).
4. Eliminar Atlas tras el período de gracia.

---

## Rollback

Cualquier fase puede revertirse en minutos:

| Síntoma | Acción |
|---|---|
| App empieza a fallar tras Fase 2 | App revierte a `RECO_BACKEND=atlas`; el writer sigue escribiendo a ambos, no hay pérdida de datos. |
| Cosmos vCore inalcanzable | Container Apps Job re-escribe a Atlas el siguiente run (env var en el job). |
| Drift de embeddings entre backends | Forzar re-corrida full con `--stage=all` y verificar paridad otra vez. |
| Atlas ya decomisionado y aparece bug crítico | Restore del backup de Atlas (≤ 30 días) + revertir la app. |

---

## Métricas de éxito (Fase 2)

- Jaccard@10 promedio FAISS↔Cosmos ≥ 0.7 al cierre de Fase 1.
- p95 latencia búsqueda semántica ≤ 1.2× la latencia legacy con FAISS.
- 0 errores 5xx atribuibles a Cosmos durante 7 días continuos.
- Cobertura de eventos en Cosmos = cobertura en Silver Parquet (verificado
  por `validate_silver.py` y conteo simple en Cosmos).

---

## Referencias

- [BD-7](beads://turislima_datamining-qbv) — `cosmos_sink.py`
- [BD-8](beads://turislima_datamining-ds3) — vector index HNSW + `search_similar`
- [BD-12](beads://turislima_datamining-cgh) — este plan
- [BD-13](beads://turislima_datamining-roa) — decomisión Atlas + FAISS
- `cultural_pipeline/pipeline/cosmos_sink.py`
- `cultural_pipeline/scripts/compare_reco_backends.py`
- `cultural_pipeline/scripts/azure_provision.sh`
