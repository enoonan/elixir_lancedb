defmodule ElixirLanceDB.Protocol.DbConnection do
  @moduledoc false
  use DBConnection
  alias ElixirLanceDB.Native

  @impl true
  def connect(opts) do
    data_dir = Application.get_env(:elixir_lancedb, :data_dir, "data")
    db_name = Keyword.get(opts, :db_name, "db")

    Native.connect(Path.join(File.cwd!(), "#{data_dir}/#{db_name}"))
  end

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def ping(state) do
    {:ok, state}
  end

  @impl true
  def disconnect(_err, _state) do
    :ok
  end

  @impl true
  def handle_begin(opts, state) do
  end
end
