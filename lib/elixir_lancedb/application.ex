defmodule ElixirLanceDB.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  alias ElixirLanceDB.Repo

  use Application

  @impl true
  def start(_type, args) do
    data_dir = Path.join(File.cwd!(), Keyword.get(args, :data_dir, "data"))
    Application.put_env(:elixir_lancedb, :data_dir, data_dir)

    children = [ElixirLanceDB.Ecto.Adapters.Supervisor, Repo]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ElixirLanceDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
