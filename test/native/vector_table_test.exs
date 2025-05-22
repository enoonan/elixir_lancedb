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
    test "it can create an ivf_pq vector index", %{table: vectors} do
      {result, _} = vectors |> Native.create_index(["vector"], Index.ivf_pq())
      assert result == :ok

      assert vectors |> Native.list_indices() ==
               {:ok, [%{name: "vector_idx", columns: ["vector"], index_type: :ivf_pq}]}
    end
  end

  describe "Vector Table :: Search :: " do
    test "it can perform a vector query", %{table: vectors} do
      query = VQR.new(create_vec())
      {result, records} = vectors |> Native.vector_search(query)
      assert result == :ok
      assert records |> length() == 10
      assert records |> Enum.all?(&is_map/1)
    end

    test "it can perform a hybrid query", %{table: vectors} do
      vectors |> Native.create_index(["content"], Index.fts())

      query =
        VQR.new(create_vec())
        |> VQR.hybridize("here is a bunch of random text", "content")
        |> VQR.filter("content LIKE \"%content%\"")
        |> dbg

      {:ok, result} = vectors |> Native.hybrid_search(query)
      assert result |> is_list()
      [first | _] = result
      assert first["content"] =~ "content for row"
    end
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
