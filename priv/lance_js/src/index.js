const db = require("./db");

module.exports = {
  tableNames: db.tableNames,
  dropAllTables: db.dropAllTables,
  dropTable: db.dropTable,
  createTable: db.createTable,
  createEmptyTable: db.createEmptyTable,
};
