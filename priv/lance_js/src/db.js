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

async function createTable(uri, table, data, opts = { existOk: true }) {
  const db = await lancedb.connect(uri);
  return await db.createTable(table, data, opts);
}

async function createEmptyTable(
  uri,
  table,
  schemaCfg,
  opts = { mode: "overwrite" }
) {
  const db = await lancedb.connect(uri);
  schema = schemaUtil.fieldConfigsToSchema(schemaCfg.fields);
  return await db.createEmptyTable(table, schema, opts);
}

module.exports = {
  tableNames,
  dropAllTables,
  dropTable,
  createTable,
  createEmptyTable,
};
