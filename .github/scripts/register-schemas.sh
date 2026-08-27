#!/bin/bash
set -e

echo "📦 Registering Avro schemas in Schema Registry..."

# Параметры
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMAS_DIR="${SCHEMAS_DIR:-schemas}"
COMPATIBILITY="${COMPATIBILITY:-BACKWARD}"

# Проверяем, что SCHEMA_REGISTRY_URL задан
if [ -z "$SCHEMA_REGISTRY_URL" ]; then
    echo "❌ SCHEMA_REGISTRY_URL is not set"
    exit 1
fi

# Проверяем, что папка со схемами существует
if [ ! -d "$SCHEMAS_DIR" ]; then
    echo "❌ Schemas directory '$SCHEMAS_DIR' not found"
    exit 1
fi

# Функция для регистрации схемы
register_schema() {
    local schema_file="$1"
    local subject=$(basename "$schema_file" .avsc)
    local subject="${subject}-value"

    echo "🔄 Registering schema: $subject"

    # Читаем содержимое схемы
    local schema_content=$(cat "$schema_file")

    # Регистрируем схему
    local response=$(curl -s -X POST "$SCHEMA_REGISTRY_URL/subjects/$subject/versions" \
        -H "Content-Type: application/json" \
        -d '{
            "schema": '"$schema_content"'
        }')

    # Проверяем ответ
    if echo "$response" | grep -q "error_code"; then
        echo "❌ Failed to register $subject: $response"
        return 1
    fi

    local version=$(echo "$response" | grep -o '"version":[0-9]*' | cut -d':' -f2)
    echo "✅ Registered $subject as version $version"
}

# Регистрируем все .avsc файлы
echo "📂 Scanning for .avsc files in $SCHEMAS_DIR..."

for schema_file in "$SCHEMAS_DIR"/*.avsc; do
    if [ -f "$schema_file" ]; then
        register_schema "$schema_file"
    fi
done

echo "🎉 All schemas registered successfully!"