defmodule ElixirLanceDB.Table.TableSupervisor do
  alias ElixirLanceDB.Table.{TableRegistry, TableServer}
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def get_table(uri) do
    # If MyWorker is not using the new child specs, we need to pass a map:
    # spec = %{id: MyWorker, start: {MyWorker, :start_link, [foo, bar, baz]}}
    case Registry.lookup(TableRegistry, uri) do
      [{table_pid, _}] ->
        {:ok, table_pid}

      [] ->
        case DynamicSupervisor.start_child(__MODULE__, TableServer.child_spec(uri)) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          _ -> {:error, :unknown}
        end
    end
  end

  @impl true
  def init(init_arg) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      extra_arguments: [init_arg]
    )
  end
end
