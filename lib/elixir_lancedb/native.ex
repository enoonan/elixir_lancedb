defmodule ElixirLanceDB.Native do
  use Rustler,
    otp_app: :elixir_lancedb,
    crate: "elixir_lancedb"

  def connect(uri) when is_binary(uri), do: nnl()

  def add(_a, _b), do: nnl()

  defp nnl(), do: :erlang.nif_error(:nif_not_loaded)
end
