# 🎭 Pipeline Cultural Lima

Sistema de data mining y recomendación de eventos culturales en Lima, Perú.
Extrae, normaliza y embeddiza eventos de múltiples fuentes para alimentar una app de recomendaciones turísticas.

---

## 🏗️ Arquitectura

```
cultural_pipeline/
│
├── scrapers/
│   ├── scraper_bnp.py          ← Biblioteca Nacional del Perú
│   ├── scraper_mali.py         ← Museo de Arte de Lima (Selenium)
│   └── scraper_joinnus.py      ← Joinnus (plataforma de ticketing)
│
├── pipeline/
│   ├── normalizer.py           ← Transforma datos crudos → EventoEstandar
│   └── pipeline.py             ← Orquestador principal
│
├── embeddings/
│   └── embedder.py             ← Genera vectores semánticos + índice FAISS
│
├── scheduler/
│   └── scheduler.py            ← Daemon Python / crontab / GitHub Actions
│
├── output/
│   ├── eventos_estandar.csv    ← Dataset principal (para la app)
│   ├── eventos_estandar.json   ← Mismo dataset en JSON (para la API)
│   └── snapshots/              ← Histórico por run
│
├── embeddings/ (generado)
│   ├── vectors_latest.npy      ← Matriz de embeddings (N × 384)
│   ├── metadata_latest.json    ← Metadata de cada vector
│   └── faiss_latest.index      ← Índice FAISS para búsqueda rápida
│
├── logs/
│   ├── run_YYYYMMDD.log
│   └── stats_YYYYMMDD.json
│
├── .github/workflows/
│   └── weekly_pipeline.yml     ← GitHub Actions (opción cloud)
│
└── requirements.txt
```

---

## 📋 Esquema Estándar de Evento

Cada evento en `eventos_estandar.csv` / `.json` sigue este esquema:

| Campo | Tipo | Descripción |
|---|---|---|
| `event_id` | str | Hash único MD5 (source + url) |
| `titulo` | str | Nombre del evento |
| `descripcion` | str | Descripción completa |
| `tipo` | str | Categoría normalizada (ver tabla abajo) |
| `fecha_inicio` | date | ISO 8601 (YYYY-MM-DD) |
| `fecha_fin` | date\|null | ISO 8601 o null si es un día |
| `hora_inicio` | str\|null | "HH:MM" en 24h |
| `lugar` | str | Nombre del lugar/venue |
| `direccion` | str\|null | Dirección física |
| `imagen_url` | str\|null | URL de imagen principal |
| `precio` | str | "Gratuito" \| "Pago" \| "Consultar" |
| `url_evento` | str | URL canónica del evento |
| `fuente` | str | "bnp" \| "mali" \| "joinnus" |
| `ciudad` | str | "Lima" |
| `tags` | list[str] | Etiquetas semánticas adicionales |
| `texto_embedding` | str | Texto enriquecido para embeddings |
| `scraped_at` | datetime | Timestamp de extracción |

### Tipos de evento normalizados
`concierto` · `exposición` · `teatro` · `cine` · `taller` · `conferencia`
`danza` · `familia` · `tour` · `gastronomía` · `deporte` · `cultural`

---

## 🧠 Sistema de Embeddings

### ¿Por qué embeddings?

Los embeddings capturan el **significado semántico** de los eventos, no solo palabras clave.
Esto permite que el sistema de recomendación entienda que:

- "concierto de jazz" y "música en vivo" son conceptualmente similares
- "actividades para niños" incluye talleres, cine, teatro infantil
- Un usuario que pregunta "algo cultural y gratuito este finde" puede recibir
  tanto una exposición en el MALI como una charla en la BNP

### Modelo: `paraphrase-multilingual-MiniLM-L12-v2`
- ✅ Soporta **español** nativamente
- ✅ Corre **localmente** (sin costo de API)
- ✅ Dimensión 384 — balance velocidad/calidad
- ✅ Embeddings **normalizados** → cosine similarity = dot product

### Campo `texto_embedding`
Cada evento tiene un campo de texto enriquecido construido así:
```
Evento: <titulo> | Tipo: <tipo> | Lugar: <lugar> | Ciudad: Lima, Perú |
Descripción: <descripcion> | Precio: <precio> | Tags: <tags>
```

### Búsqueda semántica de ejemplo
```python
from embeddings.embedder import search_events

resultados = search_events(
    "actividades gratuitas para familias con niños",
    top_k=5
)
for r in resultados:
    print(f"[{r['score']:.3f}] {r['titulo']} — {r['lugar']} ({r['precio']})")
```

### Validación de calidad de embeddings

Después de cada corrida que genere embeddings, valida calidad con:

```bash
cd cultural_pipeline
python scripts/validate_embeddings.py --run-id latest
```

El validador genera:
- `embeddings/quality_<run_id>.json`
- `embeddings/quality_latest.json` (symlink)
- `logs/output_quality_<run_id>.json` (calidad de output canónico)
- `logs/output_quality_latest.json`

Checks incluidos:
- Integridad estructural: shape de vectores, dimensión esperada y alineación con metadata
- Sanidad numérica: ausencia de NaN/Inf y norma L2 cercana a 1 (embeddings normalizados)
- Distribución semántica: similitud top-1 por item y conteo de near-duplicates por cosine
- Recuperación intrínseca: `self_recall@1` y `self_recall@k` usando el título como query
- Calidad del catálogo canónico: columnas obligatorias, duplicados por `entity_id`, nulos en campos críticos y conteos por fuente/tipo
- Deriva de cobertura por fuente: comparación con la corrida previa para detectar caídas abruptas

Para `entity_type=place`, el `texto_embedding` incorpora señales adicionales:
- distrito y dirección
- categoría Google y rating
- resumen de reseñas destacadas

Esto mejora la separación semántica de lugares con nombres parecidos.

Estados del reporte:
- `PASS`: checks críticos correctos y métricas en rango
- `WARN`: estructura correcta con señales de degradación semántica
- `FAIL`: inconsistencia crítica (shape, dimensión o valores no finitos)

Parámetros útiles:

```bash
python scripts/validate_embeddings.py \
  --run-id latest \
  --top-k 5 \
  --sample-size 300 \
  --dup-threshold 0.95
```

Validación del output canónico:

```bash
python scripts/validar_output.py \
  --expected-sources bnp mali joinnus places \
  --emit-json
```

### Decisiones Tomadas y Justificación

1. Catálogo unificado para eventos y lugares (`entity_type`):
  Se decidió consolidar eventos y places en un solo contrato para evitar pipelines paralelos y lógica duplicada en consumo web/reco.
  Resultado: una sola fuente de verdad con filtros por tipo.

2. Google Places como fuente estática (payload JSON unificado):
  Se decidió usar `google_places_payload.json` (construido desde CSV) para garantizar reproducibilidad y desacoplar la operación diaria de disponibilidad de API externa.
  Resultado: corridas deterministas y auditables.

3. Texto de embedding enriquecido para `place`:
  Se decidió incluir distrito, dirección, categoría, rating y resumen de reseñas en `texto_embedding` de lugares para mejorar discriminación entre nombres parecidos.
  Resultado: caída fuerte de near-duplicates y mejor recuperación semántica contextual.

4. Vector store con metadata mínima además de `id + embedding`:
  Se decidió guardar filtros operativos (`entity_type`, `tipo`, `fecha_inicio`, `precio`, `fuente`, `ciudad`, `url_origen`) junto al vector para resolver búsqueda y filtrado en una sola lectura.
  Resultado: menor latencia en serving y menor dependencia de joins adicionales.

5. Validación de embeddings con checks estructurales y semánticos:
  Se decidió validar no solo shape/norma, sino también recall intrínseco y near-duplicates para detectar degradaciones tempranas.
  Resultado: criterio objetivo para `PASS/WARN/FAIL` por corrida.

6. Query de validación específica para places:
  Se decidió evaluar places con query desambiguada (`titulo + distrito/categoría/dirección`) en vez de solo título, porque muchos lugares comparten nombres genéricos.
  Resultado: métrica de recuperación más representativa del uso real.

7. Inactivación condicionada en runs parciales:
  Se decidió no limpiar documentos cuando la corrida no incluye todas las fuentes para evitar falsos negativos operativos.
  En corridas completas semanales, la limpieza elimina automáticamente documentos que no aparecen por N corridas completas consecutivas (ventana de seguridad).
  Resultado: consistencia del catálogo en ejecuciones parciales y sin crecimiento infinito en runs completos.

8. Concurrencia conservadora de scrapers (`max_workers=2`):
  Se decidió priorizar estabilidad y respeto a fuentes externas sobre throughput máximo.
  Resultado: menos riesgo de bloqueos, timeouts y comportamiento anti-bot.

---

## ⏰ Ejecución Automática (Domingo 2 AM)

### Política de limpieza automática en MongoDB

En corridas completas (todas las fuentes), el pipeline aplica limpieza automática de documentos stale:
- Si `MONGO_HARD_DELETE_STALE=true` incrementa un contador de ausencias (`missing_full_runs`) y elimina solo cuando alcanza el umbral configurado.
- Si `MONGO_HARD_DELETE_STALE=false` solo marca esos documentos como inactivos.

Parámetros:
- `MONGO_HARD_DELETE_STALE=true|false`
- `MONGO_DELETE_AFTER_MISSED_FULL_RUNS=2` (default recomendado)

Con el valor recomendado (`2`), un documento se borra recién cuando falta en 2 corridas completas consecutivas.
Si vuelve a aparecer en una corrida completa, su contador de ausencias se reinicia a `0`.

La limpieza se omite en `--dry-run` y en corridas parciales (`--sources ...`) para no borrar datos por ejecuciones incompletas.

### Opción A — GitHub Actions (recomendado para producción)

1. Sube el repo a GitHub
2. El workflow `.github/workflows/weekly_pipeline.yml` se activa automáticamente
3. Si usas OpenAI como fallback: `Settings → Secrets → OPENAI_API_KEY`

```yaml
schedule:
  - cron: "0 7 * * 0"   # 07:00 UTC = 02:00 AM Lima (UTC-5)
```

### Opción B — Servidor Linux (crontab)

```bash
python scheduler/scheduler.py --install-cron
# o manualmente: crontab -e
# 0 2 * * 0 python /ruta/pipeline/pipeline.py >> /ruta/logs/cron.log 2>&1
```

### Opción C — Daemon Python (en servidor 24/7)

```bash
python scheduler/scheduler.py   # proceso permanente
```

---

## 🚀 Instalación y Uso

```bash
# 1. Clonar repo
git clone <tu-repo>
cd cultural_pipeline

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar pipeline completo
python pipeline/pipeline.py

# 4. Solo un scraper
python pipeline/pipeline.py --sources bnp joinnus

# 5. Sin embeddings (más rápido, para pruebas)
python pipeline/pipeline.py --skip-embeddings

# 6. Probar con datos existentes
python pipeline/pipeline.py --dry-run

# 7. Buscar eventos semánticamente
python -c "
from embeddings.embedder import search_events
for r in search_events('jazz en vivo Lima', top_k=3):
    print(r['titulo'], r['score'])
"
```

## ✅ Modo Funcional (primero)

Antes de atacar configuraciones finas (Mongo, runner, schedule), ejecuta esta ruta minima:

```bash
cd cultural_pipeline

# usar el python del entorno activo; opcional: export PYTHON_BIN=/ruta/python
PYTHON_BIN=python bash scripts/run_funcional.sh
```

Opciones por variable de entorno:
- `SOURCES` (default: `"bnp joinnus mali"`)
- `DRY_RUN` (default: `false`)
- `SKIP_EMBEDDINGS` (default: `true`)
- `WRITE_MONGO_WEB` (default: `false`)
- `WRITE_MONGO_RECO` (default: `false`)

Ejemplo con Mongo web habilitado:

```bash
cd cultural_pipeline
PYTHON_BIN=python WRITE_MONGO_WEB=true bash scripts/run_funcional.sh
```

El script corre el pipeline y luego valida salidas con `scripts/validar_output.py`.

---

## 📊 Outputs del Pipeline

Por cada ejecución se generan:

| Archivo | Descripción |
|---|---|
| `output/eventos_estandar.csv` | Dataset principal actualizado |
| `output/eventos_estandar.json` | Mismo en JSON para APIs |
| `output/snapshots/eventos_YYYYMMDD.csv` | Histórico inmutable |
| `output/raw/<fuente>_YYYYMMDD.csv` | Datos crudos para auditoría |
| `embeddings/vectors_latest.npy` | Matriz de vectores |
| `embeddings/metadata_latest.json` | Metadata de cada vector |
| `embeddings/faiss_latest.index` | Índice FAISS |
| `logs/run_YYYYMMDD.log` | Log completo |
| `logs/stats_YYYYMMDD.json` | Estadísticas del run |
| `logs/output_quality_YYYYMMDD.json` | Reporte de calidad del output canónico |

---

## 🔌 Integración con la App

La app de recomendaciones puede consumir los datos de dos formas:

### 1. Archivo estático (simple)
```python
import pandas as pd
df = pd.read_csv("output/eventos_estandar.csv")
```

### 2. Búsqueda semántica (recomendado)
```python
from embeddings.embedder import search_events

# El usuario escribe libremente en la app:
query = "quiero algo para este domingo con mi familia, gratis"
eventos = search_events(query, top_k=10)
```

### 3. API REST (próxima fase)
El JSON output puede servirse directamente con FastAPI:
```python
# app_api.py (ejemplo mínimo)
from fastapi import FastAPI
from embeddings.embedder import search_events

app = FastAPI()

@app.get("/recomendar")
def recomendar(q: str, top_k: int = 5):
    return search_events(q, top_k=top_k)
```
