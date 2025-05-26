defmodule ElixirLanceDB.Otp.ConnectionServer do
  use GenServer
  alias ElixirLanceDB.Native.Table.OptimizeAction.All
  alias ElixirLanceDB.Native, as: LanceDB
  alias ElixirLanceDB.Native.Schema

  defmodule State do
    defstruct [:conn, tables: %{}]

    @type t() :: %__MODULE__{
            conn: reference() | nil,
            tables: %{}
          }
  end

  def start_link(state \\ %{}) do
    GenServer.start_link(__MODULE__, state)
  end

  @impl true
  def init(%{conn: conn, tables: tables}) do
    {:ok, %State{conn: conn, tables: tables}}
  end

  @impl true
  def handle_call({:create_empty_table, tbl_name, %Schema{} = schema}, _, state) do
    case state.conn |> LanceDB.create_empty_table(tbl_name, schema) do
      {:ok, table_conn} -> {:reply, :ok, state |> add_table(tbl_name, table_conn)}
      err -> err
    end
  end

  def handle_call(:get_conn, _, state) do
    {:reply, state.conn, state}
  end

  def handle_call({:table_ref, table_name}, _, state) do
    state |> simple_table_op(table_name, fn table -> table end)
  end

  def handle_call({:create_index, table_name, columns, index_cfg}, _, state) do
    state
    |> simple_table_op(table_name, fn table ->
      table |> LanceDB.create_index(columns, index_cfg)
    end)
  end

  def handle_call({:drop_table, table_name}, _, %State{} = state) do
    case state.conn |> LanceDB.drop_table(table_name) do
      {:ok, _} -> {:reply, :ok, %State{state | tables: state.tables |> Map.delete(table_name)}}
    end
  end

  def handle_call(:drop_all_tables, _, %State{} = state) do
    case state.conn |> LanceDB.drop_all_tables() do
      {:ok, _} -> {:reply, :ok, %State{state | tables: %{}}}
      err -> err
    end
  end

  def handle_call(:table_names, _, state) do
    case state.conn |> LanceDB.table_names() do
      {:ok, names} -> {:reply, {:ok, names}, state}
      err -> {:reply, err, state}
    end
  end

  def handle_call({:list_indices, table_name}, _, state) do
    state |> simple_table_op(table_name, fn table -> table |> LanceDB.list_indices() end)
  end

  def handle_call({:count_rows, table_name, filter}, _, state) do
    state
    |> simple_table_op(table_name, fn table -> table |> LanceDB.count_rows(filter) end)
  end

  def handle_call({:query, table_name, query_request}, _, state) do
    state
    |> simple_table_op(table_name, fn table -> table |> LanceDB.query(query_request) end)
  end

  def handle_call({:optimize, table_name}, _, state) do
    state |> simple_table_op(table_name, fn table -> table |> LanceDB.optimize(%All{}) end)
  end

  def handle_call({:vector_search, table_name, vector_query_request}, _, state) do
    state
    |> simple_table_op(table_name, fn table ->
      table |> LanceDB.vector_search(vector_query_request)
    end)
  end

  def handle_call({:add, table_name, data}, _, %State{} = state) do
    with {:ok, {new_state, table}} <- state |> with_table(table_name),
         :ok <- table |> LanceDB.add(data |> string_keys()) do
      {:reply, :ok, new_state}
    else
      {:error, {:lance_table_not_found, _}} ->
        case state.conn |> LanceDB.create_table(table_name, data |> string_keys) do
          {:ok, table_conn} -> {:reply, :ok, state |> add_table(table_name, table_conn)}
          err -> {:reply, err, state}
        end

      err ->
        {:reply, err, state}
    end
  end

  def handle_call({:delete, table_name, filter}, _, %State{} = state) when is_binary(filter) do
    state |> simple_table_op(table_name, fn table -> table |> LanceDB.delete(filter) end)
  end

  def handle_call({:update, table_name, update_cfg}, _, %State{} = state) do
    state
    |> simple_table_op(
      table_name,
      fn table -> table |> LanceDB.update(update_cfg) end
    )
  end

  def handle_call({:merge_insert, table_name, data, merge_insert_cfg}, _, %State{} = state) do
    state
    |> simple_table_op(
      table_name,
      fn table -> table |> LanceDB.merge_insert(data |> string_keys, merge_insert_cfg) end
    )
  end

  def handle_call({:create_table, tbl_name, data}, _from, %State{} = state) when is_list(data) do
    case state.conn |> LanceDB.create_table(tbl_name, data |> string_keys) do
      {:ok, table_conn} -> {:reply, :ok, state |> add_table(tbl_name, table_conn)}
      err -> err
    end
  end

  def handle_call({:schema, table_name}, _, state) do
    state
    |> simple_table_op(
      table_name,
      fn table -> table |> LanceDB.schema() end
    )
  end

  def handle_call(:state, _, state) do
    {:reply, state, state}
  end

  defp simple_table_op(state, table_name, op) when is_function(op, 1) do
    with {:ok, {new_state, table}} when is_reference(table) <- state |> with_table(table_name),
         {:ok, result} <- table |> op.() do
      {:reply, {:ok, result}, new_state}
    else
      {:ok, {_, nil}} -> {:reply, {:error, "Table #{table_name} not found"}, state}
      err -> {:reply, err, state}
    end
  end

  defp string_keys(data) when is_list(data) do
    data |> Enum.map(fn d -> d |> Jason.encode!() |> Jason.decode!() end)
  end

  defp add_table(%State{} = state, table_name, table_conn) when is_reference(table_conn) do
    %State{state | tables: state.tables |> Map.put(table_name, table_conn)}
  end

  defp with_table(%State{} = state, table_name) do
    {:ok, {state, Map.get(state.tables, table_name)}}
  end
end
