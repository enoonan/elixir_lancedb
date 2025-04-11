defmodule ElixirLanceDB.Server do
  use GenServer

  @moduledoc """
  Documentation for `ElixirLanceDB.Server`.
  """

  def start_link(default) do
    GenServer.start_link(__MODULE__, default, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_dir =
      case Keyword.get(opts, :data_dir) do
        nil -> raise ":data_dir is required"
        dir -> dir
      end

    {:ok, %{data_dir: data_dir}}
  end

  def get_dir() do
    GenServer.call(__MODULE__, :get_dir)
  end

  @impl true
  def handle_call(:get_dir, _, state) do
    {:reply, state.data_dir, state}
  end
end
