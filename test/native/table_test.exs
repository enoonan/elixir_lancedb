defmodule ElixirNativeDB.Native.TableTest do
  alias ElixirLanceDB.Native
  use ExUnit.Case

  setup do
    {:ok, conn} = Path.join(File.cwd!(), "data/testing") |> Native.connect()
    conn |> Native.drop_all_tables()
    %{conn: conn}
  end

  describe "Table :: Read ::" do
    test "it can scan for full table results", %{conn: conn} do
      items = [%{"foo" => "bar"}, %{"foo" => "baz"}]
      conn |> Native.create_table("to_open", items)
      {:ok, table} = conn |> Native.open_table("to_open")
      {:ok, results} = table |> Native.query()
      assert results == items
    end
  end
end
