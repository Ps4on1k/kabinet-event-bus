#!/bin/bash
set -e

echo "🔍 Checking Avro schemas compatibility..."

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMAS_DIR="${SCHEMAS_DIR:-schemas}"

if [ -z "$SCHEMA_REGISTRY_URL" ]; then
    echo "⚠️ SCHEMA_REGISTRY_URL not set, skipping compatibility check"
    exit 0
fi

check_compatibility() {
    local schema_file="$1"
    local subject=$(basename "$schema_file" .avsc)
    local subject="${subject}-value"

    echo "🔎 Checking compatibility for: $subject"

    local schema_content=$(cat "$schema_file")

    # Проверяем совместимость
    local response=$(curl -s -X POST "$SCHEMA_REGISTRY_URL/compatibility/subjects/$subject/versions" \
        -H "Content-Type: application/json" \
        -d '{
            "schema": '"$schema_content"'
        }')

    local is_compatible=$(echo "$response" | grep -o '"is_compatible":[^,}]*' | cut -d':' -f2)

    if [ "$is_compatible" = "true" ]; then
        echo "✅ $subject is compatible"
    else
        echo "❌ $subject is NOT compatible!"
        echo "$response"
        exit 1
    fi
}

for schema_file in "$SCHEMAS_DIR"/*.avsc; do
    if [ -f "$schema_file" ]; then
        check_compatibility "$schema_file"
    fi
done

echo "🎉 All schemas are compatible!"