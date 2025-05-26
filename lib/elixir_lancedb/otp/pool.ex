defmodule ElieixrLanceDB.Otp.Pool do
  alias ElixirLanceDB.Native.Schema
  alias ElixirLanceDB.Native.Table.{VectorQueryRequest, UpdateConfig, QueryRequest}
  alias ElixirLanceDB.Native, as: LanceDB

  def child_specs(opts \\ []) do
    if Keyword.get(opts, :conn_uri) |> is_nil do
      raise "LanceDB conn_uri is required"
    end

    conn =
      case LanceDB.connect(lance_uri()) do
        {:ok, conn} -> conn
        err -> raise "Failed to connect to LanceDB: #{err |> inspect}"
      end

    table_names =
      case conn |> LanceDB.table_names() do
        {:ok, names} -> names
        err -> raise "Failed to read LanceDB table names: #{err |> inspect()}"
      end

    tables =
      table_names
      |> Enum.reduce(%{}, fn name, acc ->
        case conn |> LanceDB.open_table(name) do
          {:ok, table_conn} -> Map.put(acc, name, table_conn)
          err -> raise "Failed to open connection to LanceDB table #{name}: #{err |> inspect}"
        end
      end)

    state = %{conn: conn, tables: tables}

    [
      {Poolex,
       pool_id: :lancedb_read,
       worker_module: ArcAi.Lance.DbConnectionServer,
       worker_args: [state],
       workers_count: Keyword.get(opts, :reader_workers_count, 100),
       max_overflow: Keyword.get(opts, :reader_max_overflow, 1000)},
      {Poolex,
       pool_id: :lancedb_write,
       worker_module: ArcAi.Lance.DbConnectionServer,
       worker_args: [state],
       workers_count: Keyword.get(opts, :writer_workers_count, 8),
       max_overflow: Keyword.get(opts, :writer_max_overflow, 2)}
    ]
  end

  def lance_uri() do
    cfg = Application.get_env(:arc_ai, :lancedbx)
    data_dir = Path.join(File.cwd!(), cfg[:data_dir] || raise("data_dir must be configured"))
    db_name = cfg[:db_name] || raise "db_name must be configured"
    "#{data_dir}/#{db_name}"
  end

  # Read - reading data

  def get_conn() do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, :get_conn)
    end)
  end

  def table_ref(table_name) do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, {:table_ref, table_name})
    end)
  end

  def table_names() do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, :table_names)
    end)
  end

  def schema(table_name) do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, {:schema, table_name})
    end)
  end

  def query(table_name, %QueryRequest{} = query \\ %QueryRequest{}) do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, {:query, table_name, query})
    end)
  end

  def vector_search(table_name, %VectorQueryRequest{} = request) do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, {:vector_search, table_name, request})
    end)
  end

  def list_indices(table_name) do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, {:list_indices, table_name})
    end)
  end

  def count_rows(table_name, filter \\ "true") do
    transact_pool(:lancedb_read, fn pid ->
      GenServer.call(pid, {:count_rows, table_name, filter})
    end)
  end

  # Writes - changing or adding data or table structure

  def add(table_name, item) when not is_list(item), do: add(table_name, [item])

  def add(table_name, data) when is_binary(table_name) and is_list(data) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:add, table_name, data})
    end)
  end

  def delete(table_name, filter) when is_binary(filter) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:delete, table_name, filter})
    end)
  end

  def create_empty_table(table_name, %Schema{} = schema) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:create_empty_table, table_name, schema})
    end)
  end

  def create_table(table_name, data) when is_list(data) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:create_table, table_name, data})
    end)
  end

  def drop_table(table_name) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:drop_table, table_name})
    end)
  end

  def drop_all_tables() do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, :drop_all_tables)
    end)
  end

  def update(table_name, %UpdateConfig{} = update_cfg) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:update, table_name, update_cfg})
    end)
  end

  def create_index(table_name, columns, index_cfg)
      when is_struct(index_cfg) and is_list(columns) do
    transact_pool(:lancedb_write, fn pid ->
      GenServer.call(pid, {:create_index, table_name, columns, index_cfg})
    end)
  end

  def optimize(table_name) do
    transact_pool(
      :lancedb_write,
      fn pid ->
        GenServer.call(pid, {:optimize, table_name}, 60_000)
      end,
      60_000
    )
  end

  @pools [:lancedb_read, :lancedb_write]
  defp transact_pool(pool, op, timeout \\ 5000) when pool in @pools and is_function(op, 1) do
    Task.async(fn ->
      Poolex.run(
        pool,
        fn pid ->
          try do
            op.(pid)
          catch
            e, r ->
              msg = "lancedbx pool transaction error: #{inspect(e)}, #{inspect(r)}"
              IO.inspect(msg)
              {:error, msg}
          end
        end,
        checkout_timeout: timeout
      )
    end)
    |> Task.await(timeout)
    |> case do
      {:ok, result} -> result
      err -> err
    end
  end
end
