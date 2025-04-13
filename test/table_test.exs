defmodule ElixirLanceDB.TableTest do
  use ExUnit.Case
  alias ElixirLanceDB.{DB, Schema, Table}
  alias ElixirLanceDB.Schema.Field
  alias ElixirLanceDB.Table.IndexConfig

  setup do
    schema = Schema.from([Field.utf8("foo")])
    {:ok, _} = DB.create_empty_table("test", schema)
    :ok
  end

  describe "tables :: " do
    test "it can add data to a table (and count rows)" do
      {:ok, _} = Table.add("test", [%{foo: "bar"}])
      assert Table.count_rows("test") == {:ok, 1}
    end

    test "it can return schema" do
      DB.create_empty_table(
        "schema_test",
        Schema.from([
          Field.utf8("site_id"),
          Field.utf8("external_id"),
          Field.utf8("external_url"),
          Field.utf8("content_body"),
          Field.utf8("content_type"),
          Field.utf8("content_title"),
          Field.vector("embeddings", Field.utf8("content_body"), 384)
        ])
      )

      {:ok, _} = Table.schema("schema_test")
      assert true
    end

    test "it can upsert data" do
      {:ok, _} = Table.add("test", [%{foo: "bar"}])
      {:ok, _} = Table.upsert("test", [%{foo: "bar"}, %{foo: "baz"}], :foo)
      assert {:ok, 2} == Table.count_rows("test")

      {:ok, _} = Table.upsert("test", [%{foo: "bar"}, %{foo: "baz"}], "foo")
      assert {:ok, 2} == Table.count_rows("test")
    end

    test "it can run a query" do
      items = add_two("test")
      {:ok, result} = Table.query("test")
      assert result == items
    end

    test "it can limit query results" do
      add_two("test")
      assert {:ok, [%{"foo" => "bar"}]} == Table.query("test", %{limit: 1})
    end

    test "it can use filter where exprssions" do
      add_two("test")
      assert {:ok, [%{"foo" => "baz"}]} == Table.query("test", %{where: "foo = 'baz'"})
    end

    test "it can delete a single row of data" do
      {:ok, _} = Table.add("test", [%{foo: "bar"}, %{foo: "baz"}])
      {:ok, _} = Table.delete("test", "foo = 'bar'")
      {:ok, result} = Table.query("test", %{where: "foo = 'bar'"})
      assert result |> Enum.empty?()
    end

    test "it can delete multiple rows of data" do
      {:ok, _} = Table.add("test", [%{foo: "bar"}, %{foo: "baz"}, %{foo: "buzz"}])
      {:ok, _} = Table.delete("test", "foo in ('bar', 'baz')")
      {:ok, result} = Table.query("test")
      assert result == [%{"foo" => "buzz"}]
    end

    test "it can update a single row of data" do
      {:ok, _} = Table.add("test", [%{foo: "bar"}, %{foo: "baz"}])

      {:ok, _} = Table.update("test", %{values: %{foo: "buzz"}, where: "foo = 'bar'"})
      {:ok, result} = Table.query("test")
      assert result |> length() == 2
      assert result |> Enum.any?(&(&1["foo"] == "buzz"))
      refute result |> Enum.any?(&(&1["foo"] == "bar"))
    end

    test "it can update multiple rows of data" do
      DB.create_table("multi_update", [
        %{id: 1, content: "foo"},
        %{id: 2, content: "bar"},
        %{id: 3, content: "foo"}
      ])

      {:ok, _} =
        Table.update("multi_update", %{values: %{content: "baz"}, where: "content = 'foo'"})

      {:ok, bazzes} = Table.query("multi_update", %{where: "content = 'baz'"})
      {:ok, foos} = Table.query("multi_update", %{where: "content = 'foo'"})

      assert bazzes |> length() == 2
      assert foos |> Enum.empty?()
    end

    test "it can create a vector index" do
      DB.create_table("vec_table", create_rows())
      {result, _} = Table.create_index("vec_table", IndexConfig.ivfPq("vector"))
      assert result == :ok
    end

    test "it can do (vector similarity) search" do
      DB.create_table("vec_table", create_rows())
      {ok, results} = Table.vector_search("vec_table", create_vec())
      assert ok == :ok
      # lance default
      assert results |> length() == 10
    end

    test "it can apply limit to similarity search" do
      DB.create_table("vec_table", create_rows())
      {:ok, result} = Table.vector_search("vec_table", create_vec(), %{limit: 20})
      assert result |> length() == 20
    end

    test "it can accept valid distance_types" do
      DB.create_table("vec_table", create_rows())
      {:ok, _} = Table.vector_search("vec_table", create_vec(), %{distance_type: "l2"})
      {:ok, _} = Table.vector_search("vec_table", create_vec(), %{distance_type: "cosine"})
      {:ok, _} = Table.vector_search("vec_table", create_vec(), %{distance_type: "dot"})
      # fails on hamming distance because that only works for binary vector search
      assert true
    end

    test "it can accept \"where\" filters" do
      DB.create_table("vec_table", create_rows())

      {:ok, [result]} =
        Table.vector_search("vec_table", create_vec(), %{where: "content = 'content for row 12'"})

      assert result["content"] == "content for row 12"
    end

    test "it can specify postfilter" do
      DB.create_table("vec_table", create_rows())
      {:ok, result} = Table.vector_search("vec_table", create_vec(), %{postfilter: true})
      assert result |> length == 10
    end

    defp create_rows(num \\ 256, dim_times_8 \\ 2) when is_integer(num) do
      0..num
      |> Enum.into([])
      |> Enum.map(fn n ->
        %{
          content: "content for row #{n}",
          vector: create_vec(dim_times_8)
        }
      end)
    end

    defp create_vec(dim_times_8 \\ 2) do
      1..(8 * dim_times_8) |> Enum.into([]) |> Enum.map(fn _ -> :rand.uniform() * 2 - 1 end)
    end

    defp add_two(tbl_name) do
      items = [%{"foo" => "bar"}, %{"foo" => "baz"}]
      {:ok, _} = Table.add(tbl_name, items)
      items
    end
  end
end
