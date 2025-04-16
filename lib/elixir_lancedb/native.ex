defmodule ElixirLanceDB.Native do
  alias ElixirLanceDB.Native.Schema
  use Rustler, otp_app: :elixir_lancedb, crate: "elixir_lancedb"

  def connect(_uri), do: :erlang.nif_error(:nif_not_loaded)

  def table_names(_conn_ref), do: :erlang.nif_error(:nif_not_loaded)

  def drop_all_tables(conn_ref) when is_reference(conn_ref),
    do: :erlang.nif_error(:nif_not_loaded)

  def create_empty_table(conn_ref, table_name, %Schema{})
      when is_binary(table_name) and is_reference(conn_ref) do
    :erlang.nif_error(:nif_not_loaded)
  end
end
