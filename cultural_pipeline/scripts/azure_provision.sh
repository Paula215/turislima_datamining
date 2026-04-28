#!/usr/bin/env bash
# =============================================================================
# azure_provision.sh — provisión idempotente del medallón cultural en Azure.
# =============================================================================
#
# Crea (o reutiliza) los siguientes recursos en una sola subscripción:
#
#   - Resource Group
#   - Storage Account (ADLS Gen2 con HNS=true) + filesystem `lake`
#   - Azure Container Registry (Basic)
#   - Key Vault (RBAC authorization)
#   - User-Assigned Managed Identity con roles:
#       · Storage Blob Data Contributor (sobre el Storage Account)
#       · Key Vault Secrets User       (sobre el Key Vault)
#       · AcrPull                      (sobre el ACR)
#   - Cosmos DB for MongoDB vCore (default tier=Free)
#   - Log Analytics Workspace
#   - Container Apps Environment (linkeado a Log Analytics)
#   - Container Apps Job con scheduleTrigger semanal (cron por defecto 07:00 UTC los domingos)
#
# Idempotente: re-correr no recrea ni rompe recursos existentes.
#
# Uso:
#   AZ_PREFIX=turislima AZ_REGION=eastus2 ./scripts/azure_provision.sh
#
# Requisitos:
#   - az cli >= 2.55.0 (con extensión `containerapp` y `cosmosdb-preview`)
#   - bash >= 4
#   - openssl (para generar la admin password si AZ_COSMOS_PASSWORD no se pasa)
#
# Salida:
#   cultural_pipeline/.env.azure con los refs/conexiones que el pipeline
#   necesita cuando se corre con LAKE_BACKEND=azure. Está gitignored.
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Parámetros (override por env vars)
# -----------------------------------------------------------------------------

AZ_PREFIX="${AZ_PREFIX:-turislima}"
AZ_REGION="${AZ_REGION:-eastus2}"            # Cosmos vCore Free Tier suele estar en eastus2 / brazilsouth

AZ_RG="${AZ_RG:-${AZ_PREFIX}-rg}"
AZ_STORAGE="${AZ_STORAGE:-${AZ_PREFIX}lake}"            # 3-24 lowercase, globalmente único
AZ_ADLS_FS="${AZ_ADLS_FS:-lake}"
AZ_ACR="${AZ_ACR:-${AZ_PREFIX}acr}"                     # 5-50 alfanumérico, global
AZ_KV="${AZ_KV:-${AZ_PREFIX}-kv}"                       # 3-24 alfanumérico, global
AZ_UAMI="${AZ_UAMI:-${AZ_PREFIX}-job-mi}"

AZ_COSMOS="${AZ_COSMOS:-${AZ_PREFIX}-cosmos}"
AZ_COSMOS_TIER="${AZ_COSMOS_TIER:-Free}"                # Free | M10 | M25 | M30
AZ_COSMOS_USER="${AZ_COSMOS_USER:-pipelineadmin}"
AZ_COSMOS_PASSWORD="${AZ_COSMOS_PASSWORD:-}"            # si vacío, se genera y guarda en KV

AZ_LOGS="${AZ_LOGS:-${AZ_PREFIX}-logs}"
AZ_CAE="${AZ_CAE:-${AZ_PREFIX}-cae}"
AZ_JOB="${AZ_JOB:-cultural-pipeline-job}"
AZ_ACR_IMAGE="${AZ_ACR_IMAGE:-cultural-pipeline:latest}"
AZ_JOB_CRON="${AZ_JOB_CRON:-0 7 * * 0}"
AZ_JOB_TIMEOUT_SECONDS="${AZ_JOB_TIMEOUT_SECONDS:-5400}"   # 90 min, alineado con weekly_pipeline.yml

ENV_FILE="${ENV_FILE:-cultural_pipeline/.env.azure}"

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

log()  { printf "\033[1;36m[az_provision]\033[0m %s\n" "$*" >&2; }
warn() { printf "\033[1;33m[az_provision][WARN]\033[0m %s\n" "$*" >&2; }
fail() { printf "\033[1;31m[az_provision][FAIL]\033[0m %s\n" "$*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "comando '$1' requerido y no está en PATH"
}

ensure_extensions() {
    az extension show -n containerapp >/dev/null 2>&1 || az extension add -n containerapp
    az extension show -n cosmosdb-preview >/dev/null 2>&1 || az extension add -n cosmosdb-preview
}

ensure_login() {
    if ! az account show >/dev/null 2>&1; then
        fail "az cli no autenticado — corre 'az login' primero"
    fi
}

set_secret_if_value() {
    local name="$1" value="${2:-}"
    if [ -n "$value" ]; then
        az keyvault secret set --vault-name "$AZ_KV" --name "$name" --value "$value" >/dev/null
        log "  secret/$name → set"
    else
        warn "  secret/$name → omitido (sin valor en env)"
    fi
}

# -----------------------------------------------------------------------------
# Pre-flight
# -----------------------------------------------------------------------------

require_cmd az
require_cmd openssl
ensure_login
ensure_extensions

SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
log "Subscription: $SUBSCRIPTION_ID"
log "Region:       $AZ_REGION | Prefix: $AZ_PREFIX"

# -----------------------------------------------------------------------------
# Resource Group
# -----------------------------------------------------------------------------

log "Resource Group: $AZ_RG"
az group show -n "$AZ_RG" >/dev/null 2>&1 \
    || az group create -n "$AZ_RG" -l "$AZ_REGION" -o none

# -----------------------------------------------------------------------------
# Storage Account (ADLS Gen2)
# -----------------------------------------------------------------------------

log "Storage Account (ADLS Gen2): $AZ_STORAGE"
if ! az storage account show -n "$AZ_STORAGE" -g "$AZ_RG" >/dev/null 2>&1; then
    az storage account create \
        -n "$AZ_STORAGE" -g "$AZ_RG" -l "$AZ_REGION" \
        --sku Standard_LRS \
        --hns true \
        --min-tls-version TLS1_2 \
        --allow-blob-public-access false \
        -o none
fi

STORAGE_KEY="$(az storage account keys list -n "$AZ_STORAGE" -g "$AZ_RG" --query "[0].value" -o tsv)"

log "Filesystem: $AZ_ADLS_FS (en $AZ_STORAGE)"
az storage fs show -n "$AZ_ADLS_FS" --account-name "$AZ_STORAGE" --account-key "$STORAGE_KEY" >/dev/null 2>&1 \
    || az storage fs create -n "$AZ_ADLS_FS" --account-name "$AZ_STORAGE" --account-key "$STORAGE_KEY" -o none

# Lifecycle: cool a 30 días, delete a 365 (volumen es trivial pero documentamos política)
LIFECYCLE_POLICY=$(cat <<'JSON'
{
  "rules": [{
    "enabled": true,
    "name": "lake-tiering",
    "type": "Lifecycle",
    "definition": {
      "filters": {"blobTypes": ["blockBlob"], "prefixMatch": ["lake/bronze/", "lake/silver/", "lake/gold/"]},
      "actions": {"baseBlob": {"tierToCool": {"daysAfterModificationGreaterThan": 30}, "delete": {"daysAfterModificationGreaterThan": 365}}}
    }
  }]
}
JSON
)
az storage account management-policy create \
    --account-name "$AZ_STORAGE" -g "$AZ_RG" \
    --policy "$LIFECYCLE_POLICY" >/dev/null 2>&1 || warn "lifecycle policy ya existe o falló (no crítico)"

# -----------------------------------------------------------------------------
# Azure Container Registry
# -----------------------------------------------------------------------------

log "ACR: $AZ_ACR"
az acr show -n "$AZ_ACR" -g "$AZ_RG" >/dev/null 2>&1 \
    || az acr create -n "$AZ_ACR" -g "$AZ_RG" -l "$AZ_REGION" --sku Basic -o none
ACR_LOGIN_SERVER="$(az acr show -n "$AZ_ACR" -g "$AZ_RG" --query loginServer -o tsv)"

# -----------------------------------------------------------------------------
# Key Vault (RBAC)
# -----------------------------------------------------------------------------

log "Key Vault: $AZ_KV"
az keyvault show -n "$AZ_KV" -g "$AZ_RG" >/dev/null 2>&1 \
    || az keyvault create -n "$AZ_KV" -g "$AZ_RG" -l "$AZ_REGION" \
        --enable-rbac-authorization true \
        --enable-purge-protection true \
        -o none

# El usuario actual necesita "Key Vault Secrets Officer" para escribir secrets desde aquí.
KV_ID="$(az keyvault show -n "$AZ_KV" -g "$AZ_RG" --query id -o tsv)"
CURRENT_USER_OID="$(az ad signed-in-user show --query id -o tsv 2>/dev/null || true)"
if [ -n "$CURRENT_USER_OID" ]; then
    az role assignment create \
        --assignee "$CURRENT_USER_OID" \
        --role "Key Vault Secrets Officer" \
        --scope "$KV_ID" >/dev/null 2>&1 \
        || true
fi

# -----------------------------------------------------------------------------
# User-Assigned Managed Identity + role assignments
# -----------------------------------------------------------------------------

log "Managed Identity: $AZ_UAMI"
az identity show -n "$AZ_UAMI" -g "$AZ_RG" >/dev/null 2>&1 \
    || az identity create -n "$AZ_UAMI" -g "$AZ_RG" -l "$AZ_REGION" -o none

UAMI_PRINCIPAL="$(az identity show -n "$AZ_UAMI" -g "$AZ_RG" --query principalId -o tsv)"
UAMI_CLIENT="$(az identity show -n "$AZ_UAMI" -g "$AZ_RG" --query clientId -o tsv)"
UAMI_RESOURCE_ID="$(az identity show -n "$AZ_UAMI" -g "$AZ_RG" --query id -o tsv)"

STORAGE_ID="$(az storage account show -n "$AZ_STORAGE" -g "$AZ_RG" --query id -o tsv)"
ACR_ID="$(az acr show -n "$AZ_ACR" -g "$AZ_RG" --query id -o tsv)"

log "Role assignments para MI"
for assignment in \
    "Storage Blob Data Contributor|$STORAGE_ID" \
    "Key Vault Secrets User|$KV_ID" \
    "AcrPull|$ACR_ID"
do
    role="${assignment%%|*}"
    scope="${assignment##*|}"
    az role assignment create \
        --assignee "$UAMI_PRINCIPAL" \
        --role "$role" \
        --scope "$scope" >/dev/null 2>&1 \
        || log "  · $role (ya asignado)"
done

# -----------------------------------------------------------------------------
# Cosmos DB for MongoDB vCore
# -----------------------------------------------------------------------------

if [ -z "$AZ_COSMOS_PASSWORD" ]; then
    AZ_COSMOS_PASSWORD="$(openssl rand -base64 24 | tr -d '/+=' | cut -c1-24)A1!"
    log "Cosmos admin password generada (se guarda en Key Vault)"
fi

log "Cosmos DB MongoDB vCore: $AZ_COSMOS (tier=$AZ_COSMOS_TIER)"
if ! az cosmosdb mongocluster show -n "$AZ_COSMOS" -g "$AZ_RG" >/dev/null 2>&1; then
    az cosmosdb mongocluster create \
        -n "$AZ_COSMOS" -g "$AZ_RG" -l "$AZ_REGION" \
        --tier "$AZ_COSMOS_TIER" \
        --shard-node-count 1 \
        --shard-node-disk-size-gb 32 \
        --administrator-login "$AZ_COSMOS_USER" \
        --administrator-login-password "$AZ_COSMOS_PASSWORD" \
        -o none \
        || warn "fallo creando Cosmos vCore — verifica que el tier '$AZ_COSMOS_TIER' esté disponible en '$AZ_REGION'"
fi

# Firewall: permitir IPs de servicios Azure (la regla 0.0.0.0/0.0.0.0 es la convención
# de Cosmos vCore para "todos los servicios Azure" — NO es internet abierto)
az cosmosdb mongocluster firewall-rule create \
    --cluster-name "$AZ_COSMOS" -g "$AZ_RG" \
    --rule-name AllowAzureServices \
    --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 \
    -o none 2>/dev/null || true

COSMOS_HOST="${AZ_COSMOS}.mongocluster.cosmos.azure.com"
COSMOS_URI="mongodb+srv://${AZ_COSMOS_USER}:${AZ_COSMOS_PASSWORD}@${COSMOS_HOST}/?retryWrites=false&tls=true"

# -----------------------------------------------------------------------------
# Secrets en Key Vault
# -----------------------------------------------------------------------------

log "Secrets → Key Vault"
set_secret_if_value cosmos-uri "$COSMOS_URI"
set_secret_if_value cosmos-admin-password "$AZ_COSMOS_PASSWORD"
set_secret_if_value openai-api-key            "${OPENAI_API_KEY:-}"
set_secret_if_value deepseek-api-key          "${DEEPSEEK_API_KEY:-}"
set_secret_if_value google-geocoding-api-key  "${GOOGLE_GEOCODING_API_KEY:-}"

# -----------------------------------------------------------------------------
# Log Analytics + Container Apps Environment
# -----------------------------------------------------------------------------

log "Log Analytics: $AZ_LOGS"
az monitor log-analytics workspace show -n "$AZ_LOGS" -g "$AZ_RG" >/dev/null 2>&1 \
    || az monitor log-analytics workspace create -n "$AZ_LOGS" -g "$AZ_RG" -l "$AZ_REGION" -o none

LOGS_CUSTOMER_ID="$(az monitor log-analytics workspace show -n "$AZ_LOGS" -g "$AZ_RG" --query customerId -o tsv)"
LOGS_KEY="$(az monitor log-analytics workspace get-shared-keys -n "$AZ_LOGS" -g "$AZ_RG" --query primarySharedKey -o tsv)"

log "Container Apps Environment: $AZ_CAE"
az containerapp env show -n "$AZ_CAE" -g "$AZ_RG" >/dev/null 2>&1 \
    || az containerapp env create \
        -n "$AZ_CAE" -g "$AZ_RG" -l "$AZ_REGION" \
        --logs-workspace-id "$LOGS_CUSTOMER_ID" \
        --logs-workspace-key "$LOGS_KEY" \
        -o none

# -----------------------------------------------------------------------------
# Container Apps Job (scheduleTrigger)
# -----------------------------------------------------------------------------

log "Container Apps Job: $AZ_JOB (cron='$AZ_JOB_CRON')"
JOB_IMAGE="${ACR_LOGIN_SERVER}/${AZ_ACR_IMAGE}"
if ! az containerapp job show -n "$AZ_JOB" -g "$AZ_RG" >/dev/null 2>&1; then
    az containerapp job create \
        -n "$AZ_JOB" -g "$AZ_RG" \
        --environment "$AZ_CAE" \
        --trigger-type Schedule \
        --cron-expression "$AZ_JOB_CRON" \
        --replica-timeout "$AZ_JOB_TIMEOUT_SECONDS" \
        --replica-retry-limit 1 \
        --parallelism 1 \
        --replica-completion-count 1 \
        --image "$JOB_IMAGE" \
        --cpu 1.0 --memory 2.0Gi \
        --mi-user-assigned "$UAMI_RESOURCE_ID" \
        --registry-server "$ACR_LOGIN_SERVER" \
        --registry-identity "$UAMI_RESOURCE_ID" \
        --env-vars \
            "LAKE_BACKEND=azure" \
            "AZURE_STORAGE_ACCOUNT_NAME=$AZ_STORAGE" \
            "ADLS_FILESYSTEM=$AZ_ADLS_FS" \
            "KEY_VAULT_URI=https://${AZ_KV}.vault.azure.net" \
            "AZURE_CLIENT_ID=$UAMI_CLIENT" \
        -o none \
        || warn "Container Apps Job no se creó (posible: imagen aún no publicada en ACR). Re-corre tras 'docker push'."
fi

# -----------------------------------------------------------------------------
# Salida: .env.azure (gitignored) y siguientes pasos
# -----------------------------------------------------------------------------

mkdir -p "$(dirname "$ENV_FILE")"
cat > "$ENV_FILE" <<EOF
# Generado por scripts/azure_provision.sh — NO COMMITEAR.
# Refleja estos valores en el .env local cuando uses LAKE_BACKEND=azure.

LAKE_BACKEND=azure
AZURE_STORAGE_ACCOUNT_NAME=${AZ_STORAGE}
ADLS_FILESYSTEM=${AZ_ADLS_FS}

KEY_VAULT_URI=https://${AZ_KV}.vault.azure.net
KEY_VAULT_NAME=${AZ_KV}

COSMOS_HOST=${COSMOS_HOST}
COSMOS_DB_CATALOG=catalog
COSMOS_DB_RECO=reco

ACR_LOGIN_SERVER=${ACR_LOGIN_SERVER}
ACR_NAME=${AZ_ACR}

UAMI_CLIENT_ID=${UAMI_CLIENT}
UAMI_RESOURCE_ID=${UAMI_RESOURCE_ID}

# Estos refs apuntan a secrets en Key Vault (no se exportan como valor):
#   - cosmos-uri
#   - cosmos-admin-password
#   - openai-api-key (si fue provisto)
#   - deepseek-api-key (si fue provisto)
#   - google-geocoding-api-key (si fue provisto)
EOF

log "Done."
log "Generated: $ENV_FILE"
log ""
log "Próximos pasos:"
log "  1. Construir y publicar la imagen del pipeline (BD-10):"
log "       az acr login -n $AZ_ACR"
log "       docker buildx build -t $JOB_IMAGE cultural_pipeline/"
log "       docker push $JOB_IMAGE"
log "  2. Si el job aún no existe (faltaba la imagen), re-corre este script."
log "  3. Disparar manualmente para smoke test:"
log "       az containerapp job start -n $AZ_JOB -g $AZ_RG"
log "  4. Revisar ejecuciones:"
log "       az containerapp job execution list -n $AZ_JOB -g $AZ_RG"
