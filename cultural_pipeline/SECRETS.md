# Gestión de Secretos

## Estrategia

El repositorio **nunca contiene secretos reales**.

### Desarrollo Local (`cultural_pipeline/.env`)
- Archivo ignorado por git (en `.gitignore`)
- Contiene secretos locales reales
- **Nunca commitar** este archivo
- Llenar desde `.env.example` con valores reales locales

### CI/GitHub Actions (`.github/workflows/*.yml`)
- Los secretos se configuran en **GitHub Settings → Secrets and variables → Actions**
- El workflow los inyecta como env vars en tiempo de ejecución
- Nunca hardcodear en el YAML

### Template de Ejemplo (`.env.example`)
- Plantilla sin valores sensibles
- Documentación de variables disponibles
- Sirve para configuración inicial

## Pasos para onboarding

1. **Crear `.env` local:**
   ```bash
   cp cultural_pipeline/.env.example cultural_pipeline/.env
   ```

2. **Llenar con valores reales** (ej. URIs de MongoDB, API keys):
   ```bash
   # Abrir .env y editar
   MONGO_URI_WEB=mongodb+srv://usuario:password@cluster.mongodb.net/?retryWrites=true
   MONGO_URI_RECO=mongodb+srv://usuario:password@cluster2.mongodb.net/?retryWrites=true
   ```

3. **No commitar `.env`:**
   ```bash
   git status  # Confirmar que .env NO aparece
   ```

4. **Para GitHub Actions**, agregar a **Settings → Secrets:**
   - `MONGO_URI_WEB`
   - `MONGO_URI_RECO`
   - `OPENAI_API_KEY` (si aplica)
   - etc.

## Rotación de credenciales

Si un secret fue expuesto (ej. accidentalmente commiteado):

1. **Rotar en el servicio** (ej. cambiar password en MongoDB Atlas)
2. **Actualizar `.env` local** con el nuevo valor
3. **Actualizar GitHub Secrets** con el nuevo valor
4. **Confirmar que el repos no lo tiene en el historial** (git history, etc.)

## Validación

```bash
cd cultural_pipeline
python -c "
import os
from dotenv import load_dotenv
load_dotenv()

# Test: intenta conectar a Mongo sin exponer el URI
from pymongo import MongoClient
uri = os.getenv('MONGO_URI_WEB')
if uri:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        print('✅ Conexión exitosa')
    except Exception as e:
        print(f'❌ Error: {e}')
else:
    print('⚠️ MONGO_URI_WEB no configurado')
"
```
