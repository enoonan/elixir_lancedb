defmodule ElixirLanceDB.Native do
  alias ElixirLanceDB.Native.Schema
  use Rustler, otp_app: :elixir_lancedb, crate: "elixir_lancedb"

  def connect(uri) when is_binary(uri), do: err()

  def table_names(conn) when is_reference(conn), do: err()

  def drop_all_tables(conn) when is_reference(conn),
    do: err()

  def drop_table(conn, table_name) when is_reference(conn) and is_binary(table_name), do: err()

  def create_empty_table(conn, table_name, %Schema{})
      when is_binary(table_name) and is_reference(conn) do
    err()
  end

  def create_table(conn, table_name, initial_data) do
    {:ok, schema} = initial_data |> Schema.infer()
    create_table_with_data(conn, table_name, initial_data, schema)
  end

  def create_table_with_data(_conn, _table_name, _initial_data, %Schema{}), do: err()

  def query_table(_conn, _table_name), do: err()

  # def print(term), do: err()

  def to_arrow(_records, _schema), do: err()

  defp err(), do: :erlang.nif_error(:nif_not_loaded)
end
