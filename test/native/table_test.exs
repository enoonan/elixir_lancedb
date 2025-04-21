defmodule ElixirNativeDB.Native.TableTest do
  use ExUnit.Case

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

  describe "Table :: Read ::" do
    test "it can scan for full table results", %{table: fruits} do
      {:ok, results} = fruits |> Native.query()

      assert results == fruits()
    end

    test "it can filter results using SQL", %{table: fruits} do
      good_query = QR.new() |> QR.filter_sql("id = 123")
      bad_query = QR.new() |> QR.filter_sql("klje85pjseg")

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
      refute results |> Enum.any?(& &1["name"] in ["apple", "banana"])
    end
  end

  defp apple_query(fruits) do
    query_cfg = QR.new() |> QR.filter_sql("name = 'apple'")
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
