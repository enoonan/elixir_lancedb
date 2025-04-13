defmodule ElixirLanceDB.Table do
  alias ElixirLanceDB.Caller
  alias ElixirLanceDB.Table.IndexConfig

  def schema(table_name) do
    Caller.call("schema", table_name)
  end

  def add(table_name, data) when is_list(data) do
    Caller.call("add", [table_name, data])
  end

  def add(table_name, data), do: add(table_name, [data])

  def upsert(table_name, data, merge_insert_field \\ "id")

  def upsert(table_name, data, merge_insert_field) when is_list(data) do
    Caller.call("upsert", [table_name, data, merge_insert_field])
  end

  def upsert(table_name, data, merge_insert_field) do
    upsert(table_name, [data], merge_insert_field)
  end

  def count_rows(table_name) do
    Caller.call("countRows", table_name)
  end

  def query(table_name, params \\ %{}) do
    Caller.call("query", [table_name, params])
  end

  def delete(table_name, predicate) when is_binary(predicate) do
    Caller.call("delete", [table_name, predicate])
  end

  def update(table_name, %{values: _values, where: _predicate} = update_opts) do
    Caller.call("update", [table_name, update_opts])
  end

  def create_index(table_name, %IndexConfig{} = cfg) do
    Caller.call("createIndex", [table_name, cfg])
  end

  def vector_search(table_name, vector, params \\ %{}) do
    Caller.call("vectorSearch", [table_name, vector, params])
  end
end
