defmodule ElixirLanceDB.NativeTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native
  alias ElixirLanceDB.Native.Schema
  alias ElixirLanceDB.Native.Schema.Field

  describe "Native :: Database Connection" do
    setup do
      get_conn() |> Native.drop_all_tables()
      :ok
    end

    test "it returns a db connection" do
      assert get_conn() |> is_reference()
    end

    test "it shows empty table names list" do
      {:ok, table_names} = get_conn() |> Native.table_names()
      assert table_names |> is_list()
      assert table_names |> Enum.empty?()
    end

    test "it creates a table from schema" do
      conn = get_conn()

      schema =
        Schema.from([
          Field.utf8("test_utf8"),
          Field.float32("test_float32"),
          Field.list("test_list", Field.float32("test_child_float")),
          Field.fixed_size_list("test_fixed_size_list", Field.utf8("test_child_utf8"), 16)
        ])

      {:ok, _} = conn |> Native.create_empty_table("test_table", schema)

      assert {:ok, ["test_table"]} = conn |> Native.table_names()
    end

    test "it can drop all tables" do
      conn = get_conn()
      conn |> Native.create_empty_table("table_to_drop", Schema.from([Field.utf8("foo")]))
      assert {:ok, ["table_to_drop"]} = conn |> Native.table_names()
      conn |> Native.drop_all_tables()
      assert {:ok, []} = conn |> Native.table_names()
    end
  end

  defp get_conn() do
    {:ok, conn} = Native.connect(data_uri())
    conn
  end

  defp data_uri(), do: Path.join(File.cwd!(), "data/testing")
end
