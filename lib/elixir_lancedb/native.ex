defmodule ElixirLanceDB.Native do
  use Rustler, otp_app: :elixir_lancedb, crate: "elixir_lancedb"

  def connect(_uri), do: :erlang.nif_error(:nif_not_loaded)
end
