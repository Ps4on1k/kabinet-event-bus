#!/usr/bin/env node

/**
 * Генерирует TypeScript типы из Avro-схем.
 *
 * Использование: node scripts/generate-types.js
 *
 * Считывает все .avsc файлы из schemas/ и генерирует
 * TypeScript интерфейсы в src/generated/types.ts
 */

const fs = require('fs');
const path = require('path');

const SCHEMAS_DIR = path.join(__dirname, '..', 'schemas');
const OUTPUT_FILE = path.join(__dirname, '..', 'src', 'generated', 'types.ts');

function avroTypeToTs(field) {
  if (typeof field === 'string') {
    switch (field) {
      case 'null': return 'null';
      case 'boolean': return 'boolean';
      case 'int':
      case 'long':
      case 'float':
      case 'double':
        return 'number';
      case 'bytes':
      case 'string':
        return 'string';
      default:
        return 'unknown';
    }
  }

  if (Array.isArray(field)) {
    // Union types: [\"null\", \"string\"] => string | null
    const types = field.map(t => avroTypeToTs(t));
    return types.join(' | ');
  }

  if (typeof field === 'object') {
    switch (field.type) {
      case 'record':
        return field.name || 'unknown';
      case 'enum':
        return field.symbols ? field.symbols.map(s => `'${s}'`).join(' | ') : 'string';
      case 'array':
        return `Array<${avroTypeToTs(field.items)}>`;
      case 'map':
        return `Record<string, ${avroTypeToTs(field.values)}>`;
      case 'fixed':
        return 'string';
      default:
        return 'unknown';
    }
  }

  return 'unknown';
}

function generateRecordInterface(schema) {
  const lines = [];
  const name = schema.name;
  const doc = schema.doc ? `/** ${schema.doc} */` : undefined;

  if (doc) lines.push(`  ${doc}`);
  lines.push(`  export interface ${name}Payload {`);

  for (const field of schema.fields || []) {
    const fieldType = avroTypeToTs(field.type);
    const optional = field.default !== undefined || (Array.isArray(field.type) && field.type.includes('null'));
    const fieldDoc = field.doc ? `/** ${field.doc} */` : undefined;

    if (fieldDoc) lines.push(`    ${fieldDoc}`);
    lines.push(`    ${field.name}${optional ? '?' : ''}: ${fieldType};`);
  }

  lines.push('  }');
  return lines.join('\n');
}

function generatePayloadMap(schemas) {
  const entries = schemas.map(s => `    ${s.name}: ${s.name}Payload;`);
  return [
    '  export interface EventPayloadMap {',
    ...entries,
    '    [eventType: string]: Record<string, unknown>;',
    '  }',
  ].join('\n');
}

function main() {
  if (!fs.existsSync(SCHEMAS_DIR)) {
    console.log('📂 schemas/ directory not found, skipping type generation');
    // Create minimal generated types if schemas dir doesn't exist
    const minimalContent = [
      '/**',
      ' * Сгенерированные типы из Avro-схем.',
      ' * Файл создан автоматически. Не редактировать вручную.',
      ' */',
      '',
      '// Нет схем для генерации. Создайте .avsc файлы в schemas/',
    ].join('\n');

    fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true });
    fs.writeFileSync(OUTPUT_FILE, minimalContent, 'utf-8');
    return;
  }

  const schemaFiles = fs.readdirSync(SCHEMAS_DIR)
    .filter(f => f.endsWith('.avsc'))
    .sort();

  if (schemaFiles.length === 0) {
    console.log('📂 No .avsc files found in schemas/');
    return;
  }

  const schemas = [];

  for (const file of schemaFiles) {
    const filePath = path.join(SCHEMAS_DIR, file);
    const content = fs.readFileSync(filePath, 'utf-8');
    const schema = JSON.parse(content);

    if (schema.type !== 'record') {
      console.warn(`⚠️ Skipping ${file}: not a record type`);
      continue;
    }

    schemas.push(schema);
    console.log(`  📄 Parsed ${file} → ${schema.name}Payload`);
  }

  // Generate output
  const content = [
    '/**',
    ' * Сгенерированные типы из Avro-схем.',
    ' * Файл создан автоматически командой: npm run generate:schemas',
    ' * Не редактировать вручную.',
    ' */',
    '',
    '// ============ Типы payload-ов событий ============',
    '',
    ...schemas.map(s => generateRecordInterface(s)),
    '',
    '// ============ Расширенная карта событий ============',
    '',
    generatePayloadMap(schemas),
    '',
  ].join('\n');

  fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true });
  fs.writeFileSync(OUTPUT_FILE, content, 'utf-8');

  console.log(`✅ Generated types for ${schemas.length} schema(s) → ${path.relative(process.cwd(), OUTPUT_FILE)}`);
}

try {
  main();
} catch (error) {
  console.error('❌ generate-types error:', error.message);
  process.exit(1);
}