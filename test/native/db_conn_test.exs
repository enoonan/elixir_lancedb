defmodule ElixirLanceDB.NativeTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native
  alias ElixirLanceDB.Native.Schema
  alias ElixirLanceDB.Native.Schema.Field

  describe "Native :: Database Connection" do
    setup do
      {:ok, conn} = Path.join(File.cwd!(), "data/testing") |> Native.connect()
      conn |> Native.drop_all_tables()
      %{conn: conn}
    end

    test "it returns a db connection", %{conn: conn} do
      assert conn |> is_reference()
    end

    test "it shows empty table names list", %{conn: conn} do
      {:ok, table_names} = conn |> Native.table_names()
      assert table_names |> is_list()
      assert table_names |> Enum.empty?()
    end

    test "it creates a table from schema", %{conn: conn} do
      schema =
        Schema.from([
          Field.utf8("test_utf8"),
          Field.float32("test_float32"),
          Field.list("test_list", Field.float32("test_child_float")),
          Field.fixed_size_list("test_fixed_size_list", Field.utf8("test_child_utf8"), 384)
        ])

      {:ok, _} = conn |> Native.create_empty_table("test_table", schema)

      assert {:ok, ["test_table"]} = conn |> Native.table_names()
    end

    test "it creates a table from initial data", %{conn: conn} do
      items = [
        %{"foo" => "bar", "baz" => [123, 456]},
        %{"baz" => [789, 101_112], "foo" => "duuuuuu"}
      ]

      {:ok, table} = conn |> Native.create_table("test_from_data", items)
      assert {:ok, ["test_from_data"]} = conn |> Native.table_names()

      {:ok, result} = table |> Native.query()

      assert result == items
    end

    test "it can drop all tables", %{conn: conn} do
      conn |> Native.create_empty_table("table_to_drop", Schema.from([Field.utf8("foo")]))
      assert {:ok, ["table_to_drop"]} == conn |> Native.table_names()
      conn |> Native.drop_all_tables()
      assert {:ok, []} = conn |> Native.table_names()
    end

    test "it can drop single table", %{conn: conn} do
      conn |> Native.create_empty_table("table_to_drop", Schema.from([Field.utf8("foo")]))
      conn |> Native.create_empty_table("table_to_keep", Schema.from([Field.float32("bar")]))
      assert {:ok, tables} = conn |> Native.table_names()
      assert tables |> is_list()
      assert tables |> length() == 2
      conn |> Native.drop_table("table_to_drop")
      assert {:ok, ["table_to_keep"]} == conn |> Native.table_names()
    end

    test "it can acquire an open table resource", %{conn: conn} do
      conn |> Native.create_table("to_open", [%{"foo" => "bar"}, %{"foo" => "baz"}])
      {:ok, table} = conn |> Native.open_table("to_open")
      assert table |> is_reference()
    end
  end
end
