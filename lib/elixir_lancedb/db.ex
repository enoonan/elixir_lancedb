defmodule ElixirLanceDB.DB do
  alias ElixirLanceDB.{Schema, Caller}

  def table_names() do
    Caller.call("tableNames")
  end

  def create_table(table_name, data) when is_list(data) do
    Caller.call("createTable", [table_name, data])
  end

  def create_empty_table(table_name, %Schema{} = schema) do
    Caller.call("createEmptyTable", [table_name, schema])
  end

  def drop_table(table_name) do
    case Caller.call("dropTable", table_name) do
      _ -> {:ok, nil}
    end
  end

  def drop_all_tables() do
    Caller.call("dropAllTables")
  end
end
