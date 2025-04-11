defmodule ElixirLanceDB.Table.TableServer do
  alias ElixirLanceDB.Table.TableRegistry
  use GenServer

  def start_link(uri) do
    GenServer.start_link(__MODULE__, uri, name: via_tuple(uri))
  end

  def start_child(uri) do
    %{
      id: "#{__MODULE__}_#{uri}",
      start: {__MODULE__, :start_link, [uri]}
    }
  end

  @impl true
  def init(uri) do
    {:ok, %{uri: uri}}
  end

  def get_state(uri) do
    GenServer.call(uri |> via_tuple, :get_state)
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  defp via_tuple(uri), do: {:via, Registry, {TableRegistry, uri}}
end
