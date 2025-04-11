const lancedb = require("@lancedb/lancedb");
const schemaUtil = require("./schema");

async function tableNames(uri) {
  const db = await lancedb.connect(uri);
  return await db.tableNames();
}

async function dropAllTables(uri) {
  const db = await lancedb.connect(uri);
  return await db.dropAllTables();
}

async function dropTable(uri, table) {
  const db = await lancedb.connect(uri);
  return await db.dropTable(table);
}

async function createTable(uri, table, data, opts = { mode: "overwrite" }) {
  const db = await lancedb.connect(uri);
  return await db.createTable(table, data, opts);
}

async function createEmptyTable(
  uri,
  table,
  schema,
  opts = { mode: "overwrite" }
) {
  schema = schemaUtil.fieldConfigsToSchema(schema.fields);
  const db = await lancedb.connect(uri);
  return await db.createEmptyTable(table, schema, opts);
}

module.exports = {
  tableNames,
  dropAllTables,
  dropTable,
  createTable,
  createEmptyTable,
};
