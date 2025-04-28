defmodule ElixirLanceDB.Ecto.Adapters.Adapter do
  @behaviour Ecto.Adapter

  alias ElixirLanceDB.Ecto.Adapters.Supervisor

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: :noop

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type), do: {:ok, []}

  @impl Ecto.Adapter
  def init(config) do
    # Pulled from QLC via ecto_foundationdb
    log = Keyword.get(config, :log, :debug)
    stacktrace = Keyword.get(config, :stacktrace, nil)
    telemetry_prefix = Keyword.fetch!(config, :telemetry_prefix)
    telemetry = {config[:repo], log, telemetry_prefix ++ [:query]}

    {:ok, Supervisor.child_spec([]),
     %{telemetry: telemetry, stacktrace: stacktrace, opts: config}}
  end

  @impl Ecto.Adapter
  def checkout(%{pid: pid}, _config, fun) do
    Process.put({__MODULE__, pid}, true)
    result = fun.()
    Process.delete({__MODULE__, pid})
    result
  end

  @impl Ecto.Adapter
  def checked_out?(%{pid: pid}) do
    Process.get({__MODULE__, pid}) != nil
  end

  @impl Ecto.Adapter
  def loaders(_primitive_type, ecto_type) do
    [ecto_type]
  end

  @impl Ecto.Adapter
  def dumpers(_primitive_type, ecto_type) do
    [ecto_type]
  end
end
