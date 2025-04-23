defmodule ElixirLanceDB.Native.VectorTableTest do
  alias ElixirLanceDB.Native.Table.VectorQueryRequest, as: VQR
  alias ElixirLanceDB.Native
  alias ElixirLanceDB.Native.Table.Index

  use ExUnit.Case

  setup do
    {:ok, conn} = Path.join(File.cwd!(), "data/testing") |> Native.connect()
    conn |> Native.drop_all_tables()
    conn |> Native.create_table("vectors", create_rows())
    {:ok, vectors} = conn |> Native.open_table("vectors")
    %{table: vectors}
  end

  describe "Vector Table :: Indices" do
    test "it can create an ivf_pq index", %{table: vectors} do
      {result, _} = vectors |> Native.create_index(["vector"], Index.ivf_pq())
      assert result == :ok

      assert vectors |> Native.list_indices() ==
               {:ok, [%{name: "vector_idx", columns: ["vector"], index_type: :ivf_pq}]}
    end

    test "it can create a full text search index", %{table: vectors} do
      {result, _} = vectors |> Native.create_index(["content"], Index.fts())
      assert result == :ok

      assert vectors |> Native.list_indices() ==
               {:ok, [%{name: "content_idx", columns: ["content"], index_type: :fts}]}
    end

    test "it can perform a vector query", %{table: vectors} do
      query = VQR.new() |> VQR.with_vector(create_vec())
      {result, records} = vectors |> Native.vector_search(query) |> dbg()
      assert result == :ok
      assert records |> length() == 10
      assert records |> Enum.all?(&is_map/1)
    end
  end

  describe "Vector Table :: Search" do
  end

  defp create_rows(num \\ 256, dim_times_8 \\ 2) when is_integer(num) do
    0..num
    |> Enum.into([])
    |> Enum.map(fn n ->
      %{
        "content" => "content for row #{n}",
        "vector" => create_vec(dim_times_8)
      }
    end)
  end

  defp create_vec(dim_times_8 \\ 2) do
    1..(8 * dim_times_8) |> Enum.into([]) |> Enum.map(fn _ -> :rand.uniform() * 2 - 1 end)
  end
end
