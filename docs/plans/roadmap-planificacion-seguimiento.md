# Roadmap de Planificacion y Seguimiento

## Objetivo
Operar semanalmente el pipeline de datos para turismo cultural y de lugares en Lima, integrando las fuentes disponibles en un **catalogo unificado** y publicando a MongoDB Atlas con contratos estables para web y recomendacion.

## Principio de arquitectura
El sistema debe tratar **eventos** y **lugares** como entidades del mismo catalogo, diferenciadas por un campo de tipo:

- `entity_type = event`
- `entity_type = place`

Esto permite que:

- la vista web consuma un unico catalogo filtrando o combinando por tipo;
- la capa de recomendaciones use el mismo contrato semantico para buscar eventos y lugares;
- Google Places conviva con BNP, MALI y Joinnus sin romper el modelo.

## Fuentes incluidas
- BNP
- MALI
- Joinnus
- Google Places / Google Maps

## Contrato de datos objetivo
Campos comunes del catalogo:

- `entity_id`
- `entity_type`
- `titulo`
- `descripcion`
- `tipo`
- `fecha_inicio`
- `fecha_fin`
- `hora_inicio`
- `lugar`
- `direccion`
- `imagen_url`
- `precio`
- `url_origen`
- `fuente`
- `ciudad`
- `tags`
- `texto_embedding`
- `scraped_at`

Campos adicionales para lugares:

- `place_id`
- `rating`
- `ratings_total`
- `lat`
- `lng`
- `horario`
- `categoria_google`
- `nivel_servicio` o `nivel_interes`

## Regla de publicacion
- Web y reco deben leer del mismo modelo canónico.
- El catalogo debe permanecer consistente aunque una fuente no corra en un ciclo.
- Los documentos deben quedar diferenciados por `entity_type`, no por pipelines aislados.
- La persistencia debe ser idempotente por `entity_id`.

## Hitos

### M0 -- Seguridad y gobernanza
Duracion estimada: 1 dia.

Criterios de aceptacion:
- No quedan secretos reales en el repo.
- `.env` queda fuera de git.
- Las variables sensibles quedan documentadas solo en `.env.example` y secrets de CI.
- Existe criterio unico para URIs, tokens y claves.

### M1 -- Catalogo unificado base
Duracion estimada: 2 dias.

Criterios de aceptacion:
- Eventos y lugares comparten esquema base.
- `entity_type` se aplica en todo el flujo.
- El normalizador produce salidas coherentes para eventos y lugares.
- Google Places queda mapeado al mismo modelo canónico.

### M2 -- Publicacion web unificada
Duracion estimada: 1 a 2 dias.

Criterios de aceptacion:
- Se publica a MongoDB Atlas la vista web del catalogo.
- La vista web puede filtrar eventos o lugares por `entity_type`.
- La carga es idempotente por `entity_id`.
- El run parcial no desactiva entidades que no fueron parte del scope.

### M3 -- Publicacion reco / vector
Duracion estimada: 2 dias.

Criterios de aceptacion:
- Se generan embeddings estables de 384 dimensiones.
- El embedding contract se mantiene entre ciclos.
- La coleccion/vector store guarda eventos y lugares.
- Las consultas semanticas pueden devolver ambos tipos con filtros.

### M4 -- Integracion Google Places
Duracion estimada: 2 a 3 dias.

Criterios de aceptacion:
- Google Places entra como fuente operativa del pipeline.
- Los lugares quedan visibles en web y en reco.
- La normalizacion de places no rompe el flujo de eventos.
- Se documentan las diferencias entre entidad evento y entidad lugar.

### M5 -- Scheduler y CI
Duracion estimada: 1 dia.

Criterios de aceptacion:
- El pipeline corre semanalmente de forma automatizada.
- GitHub Actions o el scheduler del servidor ejecutan el mismo contrato.
- Los secretos se leen desde env/secrets, no desde el codigo.
- Existen logs y stats para auditar cada corrida.

### M6 -- Observabilidad y calidad
Duracion estimada: 1 a 2 dias.

Criterios de aceptacion:
- Se reportan conteos por fuente, tipo y entidad.
- Se identifican nulos, duplicados y cambios de cobertura.
- Se pueden comparar eventos vs lugares en el mismo catalogo.
- Queda documentado el criterio de recuperacion ante fallos de una fuente.

## Orden de ejecucion recomendado
1. Cerrar seguridad y gobernanza.
2. Consolidar el catalogo unificado con `entity_type`.
3. Validar publicacion web.
4. Validar reco/vector.
5. Integrar Google Places al mismo contrato.
6. Automatizar scheduler/CI.
7. Cerrar observabilidad y calidad.

## Decision clave
Si Google Places debe verse junto con eventos en la misma experiencia, no conviene tratarlo como un pipeline aparte. Conviene **unificar la base de catalogo** y diferenciar por `entity_type`, de modo que web y recomendaciones consuman una sola verdad.

## Decisiones implementadas y justificacion

1. Reco no guarda solo `id + vector`:
	Se conserva metadata minima y estable para filtros (`entity_type`, `tipo`, `fecha_inicio`, `precio`, `fuente`, `ciudad`, `url_origen`) porque el caso de uso requiere recuperar y filtrar en una sola operacion.
	Justificacion: reduce latencia y dependencia de joins en tiempo de consulta.

2. Places con semantica enriquecida en `texto_embedding`:
	Se incorporaron señales de contexto (`distrito`, `direccion`, `categoria_google`, `rating`, resumen de reseñas) para reducir colisiones semanticas entre nombres similares.
	Justificacion: mejora precision de similitud en lugares.

3. Validacion de calidad con enfoque dual:
	Se combinan checks de integridad numerica/estructural con recuperacion intrinseca (`recall@k`) y near-duplicates.
	Justificacion: una validacion solo estructural no detecta degradacion semantica.

4. Query intrinseca diferenciada para places:
	Para entidades `place`, la evaluacion usa query desambiguada (titulo + distrito/categoria/direccion).
	Justificacion: el titulo aislado no representa el uso real cuando hay nombres repetidos.

5. Inactivacion solo en corridas completas:
	La inactivacion de documentos no vistos se omite en runs parciales.
	Justificacion: evita desactivar entidades sanas cuando una fuente no fue parte de la corrida.

## Estado resumido (abril 2026)

- M0 Seguridad: completado a nivel de estrategia y plantillas de entorno.
- M1 Catalogo unificado: completado (`entity_type` operativo en flujo).
- M2 Publicacion web: completado.
- M3 Publicacion reco/vector: completado con contrato de embedding estable (384 dim).
- M4 Google Places: completado con payload estatico unificado y normalizacion integrada.
- M5 Scheduler/CI: completado (workflow semanal + validacion automatica de output canónico en CI).
- M6 Observabilidad/calidad: en progreso (stats por fuente/tipo + control de nulos/duplicados/caidas de cobertura por corrida).
