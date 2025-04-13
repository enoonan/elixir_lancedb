const lancedb = require("@lancedb/lancedb");

const schema = async function (db_dir, table_name) {
  const table = await get_table(db_dir, table_name);
  return await table.schema();
};

const add = async function (db_dir, table_name, data) {
  const table = await get_table(db_dir, table_name);
  return await table.add(data);
};

const countRows = async function (db_dir, table_name, filter = null) {
  const table = await get_table(db_dir, table_name);
  return await table.countRows(filter);
};

const query = async function (db_dir, table_name, params = {}) {
  const table = await get_table(db_dir, table_name);
  return apply_query_params(table.query(), params).toArray();
};

const createIndex = async function (db_dir, table_name, config) {
  const table = await get_table(db_dir, table_name);
  return await table.createIndex(config.field, {
    config: lancedb.Index.ivfPq(config.config),
  });
};

const vectorSearch = async function (db_dir, table_name, vector, params = {}) {
  const table = await get_table(db_dir, table_name);
  let query = table.search(vector);
  query = apply_query_params(query, params);

  if (params.postfilter) {
    query = query.postfilter();
  }

  return await query.distanceType(params?.distance_type || "l2").toArray();
};

const upsert = async function (
  db_dir,
  table_name,
  data,
  mergeInsertField = "id"
) {
  const table = await get_table(db_dir, table_name);
  return await table
    .mergeInsert(mergeInsertField)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute(data);
};

const delete_rows = async function (db_dir, table_name, predicate) {
  const table = await get_table(db_dir, table_name);
  return await table.delete(predicate);
};

const update = async function (db_dir, table_name, update_opts) {
  const table = await get_table(db_dir, table_name);
  return await table.update(update_opts);
};

async function get_table(db_dir, table_name) {
  const db = await lancedb.connect(db_dir);
  return await db.openTable(table_name);
}

function apply_query_params(query, params) {
  if (Number.isInteger(params.limit)) {
    query = query.limit(params.limit);
  }

  if (typeof params.where === "string") {
    query = query.where(params.where);
  }

  return query;
}

module.exports = {
  add,
  countRows,
  query,
  createIndex,
  vectorSearch,
  upsert,
  delete: delete_rows,
  update: update,
  schema,
};
