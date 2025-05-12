defmodule ElixirLanceDB.Native do
  alias ElixirLanceDB.Native.Schema.Infer
  alias ElixirLanceDB.Native.Schema.NewColumnTransform.AllNulls
  alias ElixirLanceDB.Native.Schema.ColumnAlteration
  alias ElixirLanceDB.Native.Table.OptimizeAction.All
  alias ElixirLanceDB.Native.Table.FullTextSearchQueryRequest
  alias ElixirLanceDB.Native.Table.Index.{Auto, BTree, Bitmap, LabelList, IvfPq, FTS}

  alias ElixirLanceDB.Native.Table.{
    MergeInsertConfig,
    UpdateConfig,
    QueryRequest,
    VectorQueryRequest
  }

  alias ElixirLanceDB.Native.Schema
  use Rustler, otp_app: :elixir_lancedb, crate: "elixir_lancedb"

  def connect(uri) when is_binary(uri), do: err()

  def close_db_connection(conn) when is_reference(conn), do: err()

  def table_names(conn) when is_reference(conn), do: err()

  def drop_all_tables(conn) when is_reference(conn),
    do: err()

  def drop_table(conn, table_name) when is_reference(conn) and is_binary(table_name), do: err()

  def create_empty_table(conn, table_name, %Schema{})
      when is_binary(table_name) and is_reference(conn) do
    err()
  end

  def create_table(conn, table_name, initial_data) do
    {:ok, schema} = initial_data |> Schema.infer()

    initial_data =
      case initial_data |> Infer.needs_cleaning?() do
        true -> initial_data |> Infer.clean()
        false -> initial_data
      end

    create_table_with_data(conn, table_name, initial_data, schema)
  end

  def create_table_with_data(_conn, _table_name, _initial_data, %Schema{}), do: err()

  def open_table(_conn, _table_name), do: err()

  def close_table(_table_ref), do: err()

  def schema(_table_ref), do: err()
  def add_columns(_table_ref, %AllNulls{transform_type: :all_nulls, schema: %Schema{}}), do: err()
  def drop_columns(_table_ref, columns) when is_list(columns), do: err()

  def alter_column(table_ref, %ColumnAlteration{} = col), do: alter_columns(table_ref, [col])
  def alter_columns(_table_ref, [%ColumnAlteration{} | _rest]), do: err()

  def count_rows(_table_ref, filter \\ "") when is_binary(filter), do: err()

  def optimize(_table_ref, %All{}), do: err()

  def query(_table_ref, %QueryRequest{} \\ %QueryRequest{}), do: err()

  def add(table_ref, records) do
    records =
      case records |> Infer.needs_cleaning?() do
        true -> records |> Infer.clean()
        false -> records
      end

    add_records(table_ref, records)
  end

  def add_records(_table_ref, _records), do: err()

  def update(_table_ref, %UpdateConfig{} = _update_cfg), do: err()

  def delete(_table_ref, predicate) when is_binary(predicate), do: err()

  def merge_insert(_table_ref, _records, %MergeInsertConfig{} \\ %MergeInsertConfig{}), do: err()

  def list_indices(_table_ref), do: err()

  def create_index(_table_ref, fields, cfg \\ %Auto{})
  def create_index(_table_ref, fields, %Auto{}) when is_list(fields), do: err()
  def create_index(_table_ref, fields, %BTree{}) when is_list(fields), do: err()
  def create_index(_table_ref, fields, %Bitmap{}) when is_list(fields), do: err()
  def create_index(_table_ref, fields, %LabelList{}) when is_list(fields), do: err()
  def create_index(_table_ref, fields, %IvfPq{}) when is_list(fields), do: err()
  def create_index(_table_ref, fields, %FTS{}) when is_list(fields), do: err()

  def full_text_search(_table_ref, %QueryRequest{full_text_search: %FullTextSearchQueryRequest{}}),
    do: err()

  def vector_search(_table_ref, %VectorQueryRequest{}), do: err()
  def hybrid_search(_table_ref, %VectorQueryRequest{}), do: err()

  def to_arrow(_records, _schema), do: err()

  defp err(), do: :erlang.nif_error(:nif_not_loaded)
end
