#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import process from "node:process";
import pg from "pg";

const { Client } = pg;

const GENERAL_HELP = `dbzod

Generate Zod schemas and JSDoc typedefs from a PostgreSQL database.

Usage:
  dbzod generate [options]
  dbzod list tables [options]

Commands:
  generate     Generate schemas.mjs from PostgreSQL tables.
  list tables  List PostgreSQL tables in a schema.

Options:
  -c, --connection <url>  PostgreSQL connection string. Defaults to DATABASE_URL.
  -s, --schema <schema>   PostgreSQL schema to inspect. Defaults to public.
  -h, --help              Show this help text.
`;

const GENERATE_HELP = `dbzod generate

Generate Zod schemas and JSDoc typedefs from PostgreSQL tables.

Usage:
  dbzod generate --connection <postgres-url> [--schema public] [--out schemas.mjs]
  DATABASE_URL=<postgres-url> dbzod generate

Options:
  -c, --connection <url>  PostgreSQL connection string. Defaults to DATABASE_URL.
  -s, --schema <schema>   PostgreSQL schema to inspect. Defaults to public.
  -o, --out <path>        Output file path. Defaults to schemas.mjs.
  -t, --table <pattern>   Include table name or glob pattern. Repeatable or comma-separated.
  --exclude-table <pat>   Exclude table name or glob pattern. Repeatable or comma-separated.
  -h, --help              Show this help text.
`;

const LIST_TABLES_HELP = `dbzod list tables

List PostgreSQL tables in a schema.

Usage:
  dbzod list tables --connection <postgres-url> [--schema public]
  DATABASE_URL=<postgres-url> dbzod list tables

Options:
  -c, --connection <url>  PostgreSQL connection string. Defaults to DATABASE_URL.
  -s, --schema <schema>   PostgreSQL schema to inspect. Defaults to public.
  -h, --help              Show this help text.
`;

const IDENTIFIER_WORDS = /[^a-zA-Z0-9]+/g;

function parseArgs(argv) {
  const [command, subcommand] = argv;

  if (!command || command === "-h" || command === "--help") {
    return { command: "help", helpText: GENERAL_HELP };
  }

  if (command === "generate") {
    const options = parseOptions(argv.slice(1), { allowOut: true });

    if (options.help) {
      return { command: "help", helpText: GENERATE_HELP };
    }

    return { command: "generate", ...options };
  }

  if (command === "list") {
    if (subcommand === "-h" || subcommand === "--help") {
      return { command: "help", helpText: LIST_TABLES_HELP };
    }

    if (subcommand !== "tables") {
      throw new Error("Unknown command. Use `dbzod list tables` or `dbzod --help`.");
    }

    const options = parseOptions(argv.slice(2), { allowOut: false });

    if (options.help) {
      return { command: "help", helpText: LIST_TABLES_HELP };
    }

    return { command: "list-tables", ...options };
  }

  throw new Error(`Unknown command: ${command}. Use \`dbzod --help\`.`);
}

function parseOptions(argv, { allowOut }) {
  const options = {
    connection: process.env.DATABASE_URL,
    schema: "public",
    help: false,
  };

  if (allowOut) {
    options.out = "schemas.mjs";
    options.tables = [];
    options.excludeTables = [];
  }

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "-h" || arg === "--help") {
      options.help = true;
      continue;
    }

    if (arg === "-c" || arg === "--connection") {
      options.connection = readOptionValue(argv, index, arg);
      index += 1;
      continue;
    }

    if (arg === "-s" || arg === "--schema") {
      options.schema = readOptionValue(argv, index, arg);
      index += 1;
      continue;
    }

    if (arg === "-o" || arg === "--out") {
      if (!allowOut) {
        throw new Error(`${arg} is only supported by dbzod generate`);
      }

      options.out = readOptionValue(argv, index, arg);
      index += 1;
      continue;
    }

    if (arg === "-t" || arg === "--table" || arg === "--tables") {
      if (!allowOut) {
        throw new Error(`${arg} is only supported by dbzod generate`);
      }

      options.tables.push(...readFilterValues(argv, index, arg));
      index += 1;
      continue;
    }

    if (arg === "--exclude-table" || arg === "--exclude-tables") {
      if (!allowOut) {
        throw new Error(`${arg} is only supported by dbzod generate`);
      }

      options.excludeTables.push(...readFilterValues(argv, index, arg));
      index += 1;
      continue;
    }

    throw new Error(`Unknown option: ${arg}`);
  }

  return options;
}

function readOptionValue(argv, index, option) {
  const value = argv[index + 1];

  if (!value || value.startsWith("-")) {
    throw new Error(`${option} requires a value`);
  }

  return value;
}

function readFilterValues(argv, index, option) {
  return readOptionValue(argv, index, option)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

async function introspectPostgres(connectionString, schemaName) {
  const client = new Client({ connectionString });

  await client.connect();

  try {
    const result = await client.query(
      `
        select
          c.table_name,
          c.column_name,
          c.ordinal_position,
          c.is_nullable,
          c.data_type,
          c.udt_schema,
          c.udt_name,
          c.character_maximum_length,
          c.numeric_precision,
          c.numeric_scale,
          c.datetime_precision,
          c.column_default,
          obj_description(cls.oid, 'pg_class') as table_description,
          col_description(cls.oid, att.attnum) as column_description,
          exists (
            select 1
            from information_schema.table_constraints tc
            join information_schema.key_column_usage kcu
              on tc.constraint_schema = kcu.constraint_schema
              and tc.constraint_name = kcu.constraint_name
              and tc.table_schema = kcu.table_schema
              and tc.table_name = kcu.table_name
            where tc.constraint_type = 'PRIMARY KEY'
              and tc.table_schema = c.table_schema
              and tc.table_name = c.table_name
              and kcu.column_name = c.column_name
          ) as is_primary_key
        from information_schema.columns c
        join information_schema.tables t
          on t.table_schema = c.table_schema
          and t.table_name = c.table_name
        join pg_namespace ns
          on ns.nspname = c.table_schema
        join pg_class cls
          on cls.relnamespace = ns.oid
          and cls.relname = c.table_name
        join pg_attribute att
          on att.attrelid = cls.oid
          and att.attname = c.column_name
          and att.attnum > 0
          and not att.attisdropped
        where c.table_schema = $1
          and t.table_type = 'BASE TABLE'
        order by c.table_name, c.ordinal_position
      `,
      [schemaName],
    );

    const enumLabelsByType = await loadEnumLabels(client);
    const checkConstraints = await loadCheckConstraints(client, schemaName);
    const tables = groupColumnsByTable(result.rows);

    applyEnumLabels(tables, enumLabelsByType);
    applyCheckConstraints(tables, checkConstraints);

    return tables;
  } finally {
    await client.end();
  }
}

async function loadEnumLabels(client) {
  const result = await client.query(
    `
      select
        n.nspname as enum_schema,
        t.typname as enum_name,
        json_agg(e.enumlabel order by e.enumsortorder) as labels
      from pg_type t
      join pg_enum e on e.enumtypid = t.oid
      join pg_namespace n on n.oid = t.typnamespace
      where n.nspname not in ('pg_catalog', 'information_schema')
      group by n.nspname, t.typname
      order by n.nspname, t.typname
    `,
  );

  const labelsByType = new Map();

  for (const row of result.rows) {
    labelsByType.set(`${row.enum_schema}.${row.enum_name}`, row.labels);
  }

  return labelsByType;
}

async function loadCheckConstraints(client, schemaName) {
  const result = await client.query(
    `
      select
        cls.relname as table_name,
        con.conname as constraint_name,
        pg_get_constraintdef(con.oid, true) as constraint_definition,
        coalesce(
          json_agg(att.attname order by att.attnum) filter (where att.attname is not null),
          '[]'::json
        ) as column_names
      from pg_constraint con
      join pg_class cls on cls.oid = con.conrelid
      join pg_namespace ns on ns.oid = cls.relnamespace
      left join unnest(con.conkey) as ck(attnum) on true
      left join pg_attribute att
        on att.attrelid = cls.oid
        and att.attnum = ck.attnum
      where con.contype = 'c'
        and ns.nspname = $1
      group by cls.relname, con.conname, con.oid
      order by cls.relname, con.conname
    `,
    [schemaName],
  );

  return result.rows;
}

async function listPostgresTables(connectionString, schemaName) {
  const client = new Client({ connectionString });

  await client.connect();

  try {
    const result = await client.query(
      `
        select table_name
        from information_schema.tables
        where table_schema = $1
          and table_type = 'BASE TABLE'
        order by table_name
      `,
      [schemaName],
    );

    return result.rows.map((row) => row.table_name);
  } finally {
    await client.end();
  }
}

function groupColumnsByTable(rows) {
  const tableMap = new Map();

  for (const row of rows) {
    if (!tableMap.has(row.table_name)) {
      tableMap.set(row.table_name, {
        name: row.table_name,
        description: row.table_description,
        columns: [],
      });
    }

    tableMap.get(row.table_name).columns.push({
      name: row.column_name,
      nullable: row.is_nullable === "YES",
      dataType: row.data_type,
      udtSchema: row.udt_schema,
      udtName: row.udt_name,
      characterMaximumLength: toNullableNumber(row.character_maximum_length),
      numericPrecision: toNullableNumber(row.numeric_precision),
      numericScale: toNullableNumber(row.numeric_scale),
      datetimePrecision: toNullableNumber(row.datetime_precision),
      defaultValue: row.column_default,
      isPrimaryKey: row.is_primary_key,
      description: row.column_description,
      enumLabels: null,
      checkConstraints: [],
    });
  }

  return [...tableMap.values()];
}

function applyEnumLabels(tables, enumLabelsByType) {
  for (const table of tables) {
    for (const column of table.columns) {
      const labels = enumLabelsByType.get(typeKey(column));

      if (labels) {
        column.enumLabels = labels;
      }
    }
  }
}

function applyCheckConstraints(tables, checkConstraints) {
  const tablesByName = new Map(tables.map((table) => [table.name, table]));

  for (const constraint of checkConstraints) {
    if (!Array.isArray(constraint.column_names) || constraint.column_names.length !== 1) {
      continue;
    }

    const table = tablesByName.get(constraint.table_name);

    if (!table) {
      continue;
    }

    const columnName = constraint.column_names[0];
    const column = table.columns.find((candidate) => candidate.name === columnName);

    if (!column) {
      continue;
    }

    column.checkConstraints.push({
      name: constraint.constraint_name,
      definition: constraint.constraint_definition,
      parsed: parseColumnCheckConstraint(constraint.constraint_definition, columnName),
    });
  }
}

function toNullableNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }

  return Number(value);
}

function generateSchemas(tables, schemaName) {
  const lines = [
    "/* eslint-disable */",
    "// Generated by dbzod. Do not edit by hand.",
    `// PostgreSQL schema: ${schemaName}`,
    "",
    'import { z } from "zod";',
    "",
  ];

  for (const table of tables) {
    const typeName = toPascalCase(table.name);
    const schemaNameForTable = `${toCamelCase(table.name)}Schema`;

    lines.push(`/**`);
    for (const descriptionLine of toJsDocDescriptionLines(table.description)) {
      lines.push(` * ${descriptionLine}`);
    }

    lines.push(` * @typedef {object} ${typeName}`);

    for (const column of table.columns) {
      lines.push(` * @property {${toJsDocType(column)}} ${safeJsDocProperty(column.name)}${toJsDocPropertyDescription(column.description)}`);
    }

    lines.push(` */`);
    lines.push(`export const ${schemaNameForTable} = z.object({`);

    for (const column of table.columns) {
      lines.push(`  ${quotePropertyName(column.name)}: ${toZodExpression(column)},`);
    }

    lines.push(`})${toZodDescription(table.description)};`);
    lines.push("");
  }

  lines.push("export const schemas = {");

  for (const table of tables) {
    lines.push(`  ${quotePropertyName(table.name)}: ${toCamelCase(table.name)}Schema,`);
  }

  lines.push("};");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

function toZodExpression(column) {
  if (isArrayColumn(column)) {
    let expression = `z.array(${toZodExpression(toArrayElementColumn(column))})`;

    if (column.nullable) {
      expression += ".nullable()";
    }

    expression += toZodDescription(column.description);

    return expression;
  }

  let expression = applyColumnZodChecks(baseZodExpression(column), column);

  if (column.nullable) {
    expression += ".nullable()";
  }

  expression += toZodDescription(column.description);

  return expression;
}

function baseZodExpression(column) {
  const enumValues = getColumnEnumValues(column);

  if (enumValues.length > 0) {
    return toZodEnumExpression(enumValues);
  }

  const normalizedType = normalizePostgresType(column);

  switch (normalizedType) {
    case "boolean":
      return "z.boolean()";
    case "integer":
      return "z.number().int()";
    case "number":
      return "z.number()";
    case "bigint":
      return "z.union([z.bigint(), z.string()])";
    case "date":
      return "z.coerce.date()";
    case "json":
      return "z.unknown()";
    case "uuid":
      return "z.string().uuid()";
    case "string":
      return "z.string()";
    default:
      return "z.unknown()";
  }
}

function applyColumnZodChecks(expression, column) {
  if (getColumnEnumValues(column).length > 0) {
    return expression;
  }

  const normalizedType = normalizePostgresType(column);

  if (normalizedType === "string") {
    return applyStringZodChecks(expression, column);
  }

  if (normalizedType === "integer" || normalizedType === "number") {
    return applyNumberZodChecks(expression, column);
  }

  return expression;
}

function applyStringZodChecks(expression, column) {
  const checks = collectStringLengthChecks(column);

  if (checks.minimum !== undefined && checks.maximum !== undefined && checks.minimum === checks.maximum) {
    return `${expression}.length(${checks.minimum})`;
  }

  let checkedExpression = expression;

  if (checks.minimum !== undefined && checks.minimum > 0) {
    checkedExpression += `.min(${checks.minimum})`;
  }

  if (checks.maximum !== undefined) {
    checkedExpression += `.max(${checks.maximum})`;
  }

  return checkedExpression;
}

function collectStringLengthChecks(column) {
  const checks = {};

  if (column.characterMaximumLength !== null) {
    if (column.udtName === "bpchar") {
      setLengthMinimum(checks, column.characterMaximumLength, true);
    }

    setLengthMaximum(checks, column.characterMaximumLength, true);
  }

  for (const constraint of column.checkConstraints) {
    const parsed = constraint.parsed;

    if (parsed.lengthMinimum) {
      setLengthMinimum(checks, parsed.lengthMinimum.value, parsed.lengthMinimum.inclusive);
    }

    if (parsed.lengthMaximum) {
      setLengthMaximum(checks, parsed.lengthMaximum.value, parsed.lengthMaximum.inclusive);
    }
  }

  return {
    minimum: checks.lengthMinimum?.value,
    maximum: checks.lengthMaximum?.value,
  };
}

function applyNumberZodChecks(expression, column) {
  const checks = collectNumberChecks(column);
  let checkedExpression = expression;

  if (checks.minimum) {
    checkedExpression += `.${checks.minimum.inclusive ? "gte" : "gt"}(${formatNumber(checks.minimum.value)})`;
  }

  if (checks.maximum) {
    checkedExpression += `.${checks.maximum.inclusive ? "lte" : "lt"}(${formatNumber(checks.maximum.value)})`;
  }

  if (checks.multipleOf) {
    checkedExpression += `.multipleOf(${formatNumber(checks.multipleOf)})`;
  }

  return checkedExpression;
}

function collectNumberChecks(column) {
  const checks = {};

  applyNativeNumberRange(checks, column);
  applyNumericPrecisionChecks(checks, column);

  for (const constraint of column.checkConstraints) {
    const parsed = constraint.parsed;

    if (parsed.minimum) {
      setMinimum(checks, parsed.minimum.value, parsed.minimum.inclusive);
    }

    if (parsed.maximum) {
      setMaximum(checks, parsed.maximum.value, parsed.maximum.inclusive);
    }
  }

  return checks;
}

function applyNativeNumberRange(checks, column) {
  switch (column.udtName) {
    case "int2":
      setMinimum(checks, -32768, true);
      setMaximum(checks, 32767, true);
      break;
    case "int4":
      setMinimum(checks, -2147483648, true);
      setMaximum(checks, 2147483647, true);
      break;
    default:
      break;
  }
}

function applyNumericPrecisionChecks(checks, column) {
  if (column.udtName !== "numeric" || column.numericPrecision === null) {
    return;
  }

  const scale = column.numericScale ?? 0;
  const integerDigits = column.numericPrecision - scale;
  const step = 10 ** -scale;
  const max = 10 ** integerDigits - step;

  if (Number.isFinite(max) && Math.abs(max) <= Number.MAX_SAFE_INTEGER) {
    setMinimum(checks, -max, true);
    setMaximum(checks, max, true);
  }

  if (Number.isFinite(step) && step > 0) {
    checks.multipleOf = step;
  }
}

function toJsDocType(column) {
  const baseType = baseJsDocType(column);

  if (column.nullable) {
    return `${baseType} | null`;
  }

  return baseType;
}

function baseJsDocType(column) {
  if (isArrayColumn(column)) {
    return `Array<${baseJsDocType(toArrayElementColumn(column))}>`;
  }

  const enumValues = getColumnEnumValues(column);

  if (enumValues.length > 0) {
    return enumValues.map((value) => JSON.stringify(value)).join(" | ");
  }

  const normalizedType = normalizePostgresType(column);

  switch (normalizedType) {
    case "boolean":
      return "boolean";
    case "integer":
    case "number":
      return "number";
    case "bigint":
      return "bigint | string";
    case "date":
      return "Date";
    case "json":
      return "unknown";
    case "uuid":
    case "string":
      return "string";
    default:
      return "unknown";
  }
}

function parseColumnCheckConstraint(definition, columnName) {
  const expression = sanitizeCheckDefinition(definition);
  const parsed = {};

  parseAllowedValues(expression, columnName, parsed);
  parseLengthComparisons(expression, columnName, parsed);
  parseNumericComparisons(expression, columnName, parsed);

  return parsed;
}

function sanitizeCheckDefinition(definition) {
  return definition
    .replace(/^CHECK\s*/i, "")
    .replace(/::\s*character varying/gi, "")
    .replace(/::\s*double precision/gi, "")
    .replace(/::\s*timestamp with time zone/gi, "")
    .replace(/::\s*timestamp without time zone/gi, "")
    .replace(/::\s*(?:"[^"]+"|[a-zA-Z_][\w$]*)(?:\s*\.\s*(?:"[^"]+"|[a-zA-Z_][\w$]*))?/g, "")
    .replace(/\bCOLLATE\s+"[^"]+"/gi, "");
}

function parseAllowedValues(expression, columnName, parsed) {
  const columnRef = postgresColumnRefPattern(columnName);
  const values = new Set();
  const patterns = [
    new RegExp(`${columnRef}\\s*=\\s*ANY\\s*\\(\\s*ARRAY\\s*\\[([^\\]]+)\\]\\s*\\)`, "gi"),
    new RegExp(`${columnRef}\\s+IN\\s*\\(([^)]+)\\)`, "gi"),
    new RegExp(`${columnRef}\\s*=\\s*('(?:''|[^'])*')`, "gi"),
  ];

  for (const pattern of patterns) {
    for (const match of expression.matchAll(pattern)) {
      for (const value of parseSqlStringLiterals(match[1])) {
        values.add(value);
      }
    }
  }

  if (values.size > 0) {
    parsed.allowedValues = [...values];
  }
}

function parseLengthComparisons(expression, columnName, parsed) {
  const columnRef = postgresColumnRefPattern(columnName);
  const lengthCall = `(?:char_length|character_length|length)\\s*\\(\\s*${columnRef}\\s*\\)`;
  const leftPattern = new RegExp(`${lengthCall}\\s*(>=|>|<=|<|=)\\s*${NUMERIC_VALUE_PATTERN}`, "gi");
  const rightPattern = new RegExp(`${NUMERIC_VALUE_PATTERN}\\s*(>=|>|<=|<|=)\\s*${lengthCall}`, "gi");

  for (const match of expression.matchAll(leftPattern)) {
    applyLengthComparison(parsed, match[1], Number(match[2]), false);
  }

  for (const match of expression.matchAll(rightPattern)) {
    applyLengthComparison(parsed, match[2], Number(match[1]), true);
  }
}

function parseNumericComparisons(expression, columnName, parsed) {
  const columnRef = postgresColumnRefPattern(columnName);
  const leftPattern = new RegExp(`${columnRef}\\s*(>=|>|<=|<)\\s*${NUMERIC_VALUE_PATTERN}`, "gi");
  const rightPattern = new RegExp(`${NUMERIC_VALUE_PATTERN}\\s*(>=|>|<=|<)\\s*${columnRef}`, "gi");

  for (const match of expression.matchAll(leftPattern)) {
    applyNumericComparison(parsed, match[1], Number(match[2]), false);
  }

  for (const match of expression.matchAll(rightPattern)) {
    applyNumericComparison(parsed, match[2], Number(match[1]), true);
  }
}

function applyLengthComparison(parsed, operator, value, reversed) {
  if (!Number.isFinite(value)) {
    return;
  }

  const normalizedOperator = reversed ? reverseComparator(operator) : operator;

  switch (normalizedOperator) {
    case ">=":
      setLengthMinimum(parsed, value, true);
      break;
    case ">":
      setLengthMinimum(parsed, value, false);
      break;
    case "<=":
      setLengthMaximum(parsed, value, true);
      break;
    case "<":
      setLengthMaximum(parsed, value, false);
      break;
    case "=":
      setLengthMinimum(parsed, value, true);
      setLengthMaximum(parsed, value, true);
      break;
    default:
      break;
  }
}

function applyNumericComparison(parsed, operator, value, reversed) {
  if (!Number.isFinite(value)) {
    return;
  }

  const normalizedOperator = reversed ? reverseComparator(operator) : operator;

  switch (normalizedOperator) {
    case ">=":
      setMinimum(parsed, value, true);
      break;
    case ">":
      setMinimum(parsed, value, false);
      break;
    case "<=":
      setMaximum(parsed, value, true);
      break;
    case "<":
      setMaximum(parsed, value, false);
      break;
    default:
      break;
  }
}

function reverseComparator(operator) {
  switch (operator) {
    case ">=":
      return "<=";
    case ">":
      return "<";
    case "<=":
      return ">=";
    case "<":
      return ">";
    default:
      return operator;
  }
}

function setMinimum(target, value, inclusive, key = "minimum") {
  const current = target[key];

  if (!current || value > current.value || (value === current.value && current.inclusive && !inclusive)) {
    target[key] = { value, inclusive };
  }
}

function setMaximum(target, value, inclusive, key = "maximum") {
  const current = target[key];

  if (!current || value < current.value || (value === current.value && current.inclusive && !inclusive)) {
    target[key] = { value, inclusive };
  }
}

function setLengthMinimum(target, value, inclusive) {
  const zodValue = inclusive ? Math.ceil(value) : Math.floor(value) + 1;

  if (zodValue < 0) {
    return;
  }

  setMinimum(target, zodValue, true, "lengthMinimum");
}

function setLengthMaximum(target, value, inclusive) {
  const zodValue = inclusive ? Math.floor(value) : Math.ceil(value) - 1;

  if (zodValue < 0) {
    return;
  }

  setMaximum(target, zodValue, true, "lengthMaximum");
}

function parseSqlStringLiterals(value) {
  const literals = [];

  for (const match of value.matchAll(/'((?:''|[^'])*)'/g)) {
    literals.push(match[1].replace(/''/g, "'"));
  }

  return literals;
}

function getColumnEnumValues(column) {
  if (column.enumLabels?.length > 0) {
    return column.enumLabels;
  }

  for (const constraint of column.checkConstraints) {
    if (constraint.parsed.allowedValues?.length > 0) {
      return constraint.parsed.allowedValues;
    }
  }

  return [];
}

function toZodEnumExpression(values) {
  return `z.enum([${values.map((value) => JSON.stringify(value)).join(", ")}])`;
}

function isArrayColumn(column) {
  return column.udtName?.startsWith("_");
}

function toArrayElementColumn(column) {
  return {
    ...column,
    nullable: false,
    udtName: column.udtName.slice(1),
    description: null,
    checkConstraints: [],
  };
}

function typeKey(column) {
  const typeName = column.udtName?.startsWith("_") ? column.udtName.slice(1) : column.udtName;

  return `${column.udtSchema}.${typeName}`;
}

function postgresColumnRefPattern(columnName) {
  const identifier = `(?:"${escapeRegExp(columnName.replace(/"/g, '""'))}"|${escapeRegExp(columnName)})`;

  return `\\(*\\s*${identifier}\\s*\\)*`;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function formatNumber(value) {
  if (Number.isInteger(value)) {
    return String(value);
  }

  return String(Number(value.toPrecision(15)));
}

const NUMERIC_VALUE_PATTERN = "\\(?\\s*([-+]?\\d+(?:\\.\\d+)?)\\s*\\)?";

function normalizePostgresType(column) {
  const type = column.udtName || column.dataType;

  switch (type) {
    case "bool":
      return "boolean";
    case "int2":
    case "int4":
      return "integer";
    case "int8":
      return "bigint";
    case "float4":
    case "float8":
    case "numeric":
      return "number";
    case "date":
    case "time":
    case "timetz":
    case "timestamp":
    case "timestamptz":
      return "date";
    case "json":
    case "jsonb":
      return "json";
    case "uuid":
      return "uuid";
    case "bpchar":
    case "char":
    case "varchar":
    case "text":
    case "inet":
    case "cidr":
    case "macaddr":
    case "macaddr8":
    case "bytea":
    case "interval":
      return "string";
    default:
      return "unknown";
  }
}

function toPascalCase(value) {
  const name = value
    .split(IDENTIFIER_WORDS)
    .filter(Boolean)
    .map((word) => `${word[0].toUpperCase()}${word.slice(1)}`)
    .join("");

  return ensureIdentifier(name || "Table");
}

function toCamelCase(value) {
  const pascal = toPascalCase(value);

  return `${pascal[0].toLowerCase()}${pascal.slice(1)}`;
}

function ensureIdentifier(value) {
  if (/^[a-zA-Z_$]/.test(value)) {
    return value;
  }

  return `_${value}`;
}

function quotePropertyName(value) {
  if (/^[a-zA-Z_$][a-zA-Z0-9_$]*$/.test(value)) {
    return value;
  }

  return JSON.stringify(value);
}

function safeJsDocProperty(value) {
  if (/^[a-zA-Z_$][a-zA-Z0-9_$]*$/.test(value)) {
    return value;
  }

  return `[${JSON.stringify(value)}]`;
}

function toJsDocDescriptionLines(description) {
  if (!description) {
    return [];
  }

  return String(description)
    .split(/\r?\n/)
    .map((line) => sanitizeJsDocText(line).trim())
    .filter(Boolean);
}

function toJsDocPropertyDescription(description) {
  if (!description) {
    return "";
  }

  return ` - ${sanitizeJsDocText(String(description).replace(/\s+/g, " ").trim())}`;
}

function sanitizeJsDocText(value) {
  return value.replace(/\*\//g, "*\\/");
}

function toZodDescription(description) {
  if (!description) {
    return "";
  }

  return `.describe(${JSON.stringify(description)})`;
}

function filterTables(tables, options) {
  let filteredTables = tables;

  if (options.tables.length > 0) {
    filteredTables = filteredTables.filter((table) => matchesAnyTableFilter(table, options.schema, options.tables));
  }

  if (options.excludeTables.length > 0) {
    filteredTables = filteredTables.filter((table) => !matchesAnyTableFilter(table, options.schema, options.excludeTables));
  }

  return filteredTables;
}

function matchesAnyTableFilter(table, schemaName, filters) {
  return filters.some((filter) => matchesTableFilter(table, schemaName, filter));
}

function matchesTableFilter(table, schemaName, filter) {
  const names = [table.name, `${schemaName}.${table.name}`];

  return names.some((name) => tableFilterPatternToRegExp(filter).test(name));
}

function tableFilterPatternToRegExp(filter) {
  const pattern = filter
    .split("")
    .map((character) => {
      if (character === "*") {
        return ".*";
      }

      if (character === "?") {
        return ".";
      }

      return escapeRegExp(character);
    })
    .join("");

  return new RegExp(`^${pattern}$`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.command === "help") {
    process.stdout.write(options.helpText);
    return;
  }

  if (!options.connection) {
    throw new Error("Missing PostgreSQL connection string. Use --connection or DATABASE_URL.");
  }

  if (options.command === "generate") {
    const allTables = await introspectPostgres(options.connection, options.schema);
    const tables = filterTables(allTables, options);

    if ((options.tables.length > 0 || options.excludeTables.length > 0) && tables.length === 0) {
      throw new Error("No tables matched the generate filters.");
    }

    const output = generateSchemas(tables, options.schema);

    await writeFile(options.out, output, "utf8");

    process.stdout.write(`Generated ${options.out} with ${tables.length} table schema${tables.length === 1 ? "" : "s"}.\n`);
    return;
  }

  if (options.command === "list-tables") {
    const tables = await listPostgresTables(options.connection, options.schema);

    if (tables.length > 0) {
      process.stdout.write(`${tables.join("\n")}\n`);
    }
  }
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
