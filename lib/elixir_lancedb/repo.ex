defmodule ElixirLanceDB.Repo do
  use Ecto.Repo, otp_app: :elixir_lancedb, adapter: ElixirLanceDB.Ecto.Adapters.Adapter

  # use EctoFoundationDB.Migrator
  # def migrations(), do: []
end
