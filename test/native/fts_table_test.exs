defmodule ElixirLanceDB.Native.FtsTableTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native.Table.QueryRequest
  alias ElixirLanceDB.Native.Table.Index
  alias ElixirLanceDB.Native

  setup do
    {:ok, conn} = Path.join(File.cwd!(), "data/testing") |> Native.connect()
    conn |> Native.drop_all_tables()
    conn |> Native.create_table("vectors", create_rows())
    {:ok, vectors} = conn |> Native.open_table("vectors")
    %{table: vectors}
  end

  describe "FTS Table ::" do
    test "it can create a full text search index", %{table: vectors} do
      {result, _} = vectors |> Native.create_index(["content"], Index.fts())
      assert result == :ok

      assert vectors |> Native.list_indices() ==
               {:ok, [%{name: "content_idx", columns: ["content"], index_type: :fts}]}
    end

    test "it can perform a full text search query", %{table: vectors} do
      vectors |> Native.create_index(["content"], Index.fts())
      query = QueryRequest.new() |> QueryRequest.fts("row 56", "content")
      {:ok, result} = vectors |> Native.full_text_search(query)
      assert result |> is_list()
      [first | _] = result
      assert first["content"] == "content for row 56"
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

  defp create_vec(dim_times_8) do
    1..(8 * dim_times_8) |> Enum.into([]) |> Enum.map(fn _ -> :rand.uniform() * 2 - 1 end)
  end
end
