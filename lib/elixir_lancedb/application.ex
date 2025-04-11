defmodule ElixirLanceDB.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, args) do
    data_dir = Path.join(File.cwd!(), Keyword.get(args, :data_dir, "data"))
    Application.put_env(:elixir_lancedb, :data_dir, data_dir)

    children = [
      {NodeJS.Supervisor,
       [pool_size: 4]
       |> Keyword.merge(args |> Keyword.get(:nodejs_opts, []))
       |> Keyword.put(:path, "#{:code.priv_dir(:elixir_lancedb)}/lance_js")},
      {ElixirLanceDB.Server, data_dir: data_dir},
      {Registry, name: ElixirLanceDB.Table.TableRegistry, keys: :unique, members: :auto},
      {DynamicSupervisor, name: ElixirLanceDB.Table.TableSupervisor, strategy: :one_for_one}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ElixirLanceDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
