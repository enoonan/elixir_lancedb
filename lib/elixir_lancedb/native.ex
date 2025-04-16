defmodule ElixirLanceDB.Native do
  alias ElixirLanceDB.Native.Schema
  use Rustler, otp_app: :elixir_lancedb, crate: "elixir_lancedb"

  def connect(_uri), do: nnl()

  def table_names(_conn_ref), do: nnl()

  def drop_all_tables(conn_ref) when is_reference(conn_ref),
    do: nnl()

  def drop_table(conn_ref, table_name) when is_reference(conn_ref) and is_binary(table_name), do: nnl()

  def create_empty_table(conn_ref, table_name, %Schema{})
      when is_binary(table_name) and is_reference(conn_ref) do
    nnl()
  end

  defp nnl(), do: :erlang.nif_error(:nif_not_loaded)
end
