defmodule ElixirLanceDB.Ecto.Adapters.Supervisor do
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init(_arg) do
    children = []
    Supervisor.init(children, strategy: :one_for_one)
  end
end
