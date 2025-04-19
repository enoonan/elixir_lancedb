defmodule ElixirNativeDB.Native.TableTest do
  alias ElixirLanceDB.Native.Table.QueryRequest, as: QR
  alias ElixirLanceDB.Native
  use ExUnit.Case

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

      assert results |> Enum.map(&(&1 |> Map.delete("avg_weight_oz"))) ==
               fruits() |> Enum.map(&(&1 |> Map.delete("avg_weight_oz")))
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
  end

  defp fruits() do
    [
      %{
        "id" => 123,
        "name" => "apple",
        "types" => ["red", "green"],
        "avg_weight_oz" => 5.36324
      },
      %{
        "id" => 456,
        "name" => "banana",
        "types" => ["cavendish", "plantain"],
        "avg_weight_oz" => 4.33425
      }
    ]
  end
end
