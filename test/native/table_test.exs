defmodule ElixirNativeDB.Native.TableTest do
  use ExUnit.Case

  alias ElixirLanceDB.Native.Schema.NewColumnTransform
  alias ElixirLanceDB.Native.Schema.ColumnAlteration
  alias ElixirLanceDB.Native.Schema.Field
  alias ElixirLanceDB.Native.Schema
  alias ElixirLanceDB.Native.Table.OptimizeAction.All
  alias ElixirLanceDB.Native.Table.Index
  alias ElixirLanceDB.Native
  alias ElixirLanceDB.Native.Table.QueryRequest, as: QR
  alias ElixirLanceDB.Native.Table.UpdateConfig, as: UpCfg

  setup do
    {:ok, conn} = Path.join(File.cwd!(), "data/testing") |> Native.connect()
    conn |> Native.drop_all_tables()
    conn |> Native.create_table("fruits", fruits())
    {:ok, fruits} = conn |> Native.open_table("fruits")
    %{table: fruits}
  end

  describe "Table :: Operations ::" do
    test "it can get schema", %{table: fruits} do
      {result, schema} = fruits |> Native.schema()
      assert result == :ok

      assert schema ==
               Schema.from([
                 Field.float32("avg_weight_oz"),
                 Field.int32("id"),
                 Field.utf8("name"),
                 Field.list("types", Field.utf8("item"))
               ])
    end

    test "it can drop columns", %{table: fruits} do
      fruits |> Native.drop_columns(["types", "avg_weight_oz"])
      {result, schema} = fruits |> Native.schema()
      assert result == :ok
      assert schema == Schema.from([Field.int32("id"), Field.utf8("name")])
    end

    test "it can alter columns", %{table: fruits} do
      fruits
      |> Native.alter_columns([
        ColumnAlteration.new("avg_weight_oz", rename: "avg_weight_lb"),
        ColumnAlteration.new("id", data_type: :int64),
        ColumnAlteration.new("types", nullable: true)
      ])

      {:ok, schema} = fruits |> Native.schema()
      assert schema.fields |> Enum.any?(&(&1.name == "avg_weight_lb"))
      assert schema.fields |> Enum.any?(&(&1.name == "id" and &1.field_type == :int64))
      assert schema.fields |> Enum.any?(&(&1.name == "types" and &1.nullable))
    end

    test "it can add columns", %{table: fruits} do
      new_col = NewColumnTransform.all_nulls([Field.int32("qty")])
      fruits |> Native.add_columns(new_col)
      {:ok, schema} = fruits |> Native.schema()

      assert schema.fields
             |> Enum.any?(&(&1.name == "qty" and &1.field_type == :int32))

      {:ok, all_fruits} = fruits |> Native.query()
      assert all_fruits |> Enum.all?(&(&1["qty"] == 0))
    end
  end

  describe "Table :: CRUD ::" do
    test "it can count rows", %{table: fruits} do
      assert {:ok, 2} == fruits |> Native.count_rows()
      assert {:ok, 1} == fruits |> Native.count_rows("name = 'apple'")
    end

    test "it can scan for full table results", %{table: fruits} do
      {:ok, results} = fruits |> Native.query()

      assert results == fruits()
    end

    test "it can filter results using SQL", %{table: fruits} do
      good_query = QR.new() |> QR.filter("id = 123")
      bad_query = QR.new() |> QR.filter("klje85pjseg")

      {:ok, [apple]} = fruits |> Native.query(good_query)
      {:error, {:lance, err_msg}} = fruits |> Native.query(bad_query)

      assert apple["name"] == "apple"
      assert err_msg |> String.starts_with?("Invalid user input")
    end

    test "it can limit results", %{table: fruits} do
      {:ok, result} = fruits |> Native.query(QR.new() |> QR.limit(1))
      assert result |> length() == 1
      apple? = result |> Enum.at(0)
      assert apple?["name"] == "apple"
      assert apple?["types"] == ["red", "green"]
    end

    test "it can add rows to existing table", %{table: fruits} do
      fruits |> Native.add(new_fruits())
      {:ok, full_table} = fruits |> Native.query()
      assert full_table |> length() == 4
      assert full_table |> Enum.find(fn fruit -> fruit["name"] == "grape" end)
      assert full_table |> Enum.find(fn fruit -> fruit["name"] == "orange" end)
    end

    test "it can update all rows in a table", %{table: fruits} do
      update_cfg =
        UpCfg.new()
        |> UpCfg.column("avg_weight_oz", "0")

      fruits |> Native.update(update_cfg)

      {:ok, result} = fruits |> Native.query()
      assert result |> Enum.all?(&(&1["avg_weight_oz"] == 0.0))
    end

    test "it can update a filtered row in table", %{table: fruits} do
      update_cfg =
        UpCfg.new()
        |> UpCfg.column("avg_weight_oz", "0")
        |> UpCfg.filter("name = 'apple'")

      fruits |> Native.update(update_cfg)

      {:ok, [apple]} = apple_query(fruits)
      assert(apple["avg_weight_oz"] == 0.0)
    end

    test "it can delete a record", %{table: fruits} do
      fruits |> Native.delete("id = 123")
      {:ok, [result]} = fruits |> Native.query()
      assert result["name"] == "banana"
    end

    test "it can delete multiple records", %{table: fruits} do
      fruits |> Native.add(new_fruits())
      fruits |> Native.delete("name in ('apple', 'banana')")
      {:ok, results} = fruits |> Native.query()
      assert results |> length() == 2
      refute results |> Enum.any?(&(&1["name"] in ["apple", "banana"]))
    end
  end

  describe "Table :: Indices ::" do
    test "it can list indices", %{table: fruits} do
      assert {:ok, []} == fruits |> Native.list_indices()
    end

    test "it can create an auto index", %{table: fruits} do
      {result, _} = fruits |> Native.create_index(["name"])
      assert result == :ok

      assert fruits |> Native.list_indices() ==
               {:ok, [%{name: "name_idx", columns: ["name"], index_type: :btree}]}
    end

    test "it can create other simple index types", %{table: fruits} do
      {result1, _} = fruits |> Native.create_index(["name"], Index.btree())
      {result2, _} = fruits |> Native.create_index(["id"], Index.bitmap())
      {result3, _} = fruits |> Native.create_index(["types"], Index.label_list())
      assert [result1, result2, result3] |> Enum.all?(&(&1 == :ok))

      assert fruits |> Native.list_indices() ==
               {:ok,
                [
                  %{columns: ["name"], index_type: :btree, name: "name_idx"},
                  %{columns: ["id"], index_type: :bitmap, name: "id_idx"},
                  %{columns: ["types"], index_type: :label_list, name: "types_idx"}
                ]}
    end

    test "it can run optimize with All optimizations", %{table: fruits} do
      {result, stats} = fruits |> Native.optimize(%All{})
      assert result == :ok

      assert stats ==
               %{
                 prune: %{bytes_removed: 0, old_versions: 0},
                 compaction: %{
                   fragments_removed: 0,
                   fragments_added: 0,
                   files_removed: 0,
                   files_added: 0
                 }
               }
    end
  end

  defp apple_query(fruits) do
    query_cfg = QR.new() |> QR.filter("name = 'apple'")
    fruits |> Native.query(query_cfg)
  end

  defp fruits() do
    [
      %{
        "id" => 123,
        "name" => "apple",
        "types" => ["red", "green"],
        "avg_weight_oz" => 5.363239765167236
      },
      %{
        "id" => 456,
        "name" => "banana",
        "types" => ["cavendish", "plantain"],
        "avg_weight_oz" => 4.334249973297119
      }
    ]
  end

  defp new_fruits() do
    [
      %{
        "id" => 234,
        "name" => "grape",
        "types" => ["red", "green"],
        "avg_weight_oz" => 6.345239765167236
      },
      %{
        "id" => 567,
        "name" => "orange",
        "types" => ["mandarine", "navel", "disappointing"],
        "avg_weight_oz" => 7.338769973297119
      }
    ]
  end
end
