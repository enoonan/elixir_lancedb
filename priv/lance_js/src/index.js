const db = require("./db");
const table = require("./table");

module.exports = {
  tableNames: db.tableNames,
  dropAllTables: db.dropAllTables,
  dropTable: db.dropTable,
  createTable: db.createTable,
  createEmptyTable: db.createEmptyTable,
  add: table.add,
  countRows: table.countRows,
  query: table.query,
  createIndex: table.createIndex,
  vectorSearch: table.vectorSearch,
  upsert: table.upsert,
  delete: table.delete,
  update: table.update,
  schema: table.schema,
};
