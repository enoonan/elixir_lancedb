defmodule ElixirLanceDB.DbTest do
  use ExUnit.Case
  alias ElixirLanceDB.Schema.FieldTypes.FixedSizeList
  alias ElixirLanceDB.Schema.FieldTypes.{Utf8}
  alias ElixirLanceDB.Schema.Field
  alias ElixirLanceDB.Schema
  alias ElixirLanceDB.DB

  setup do
    DB.drop_all_tables()
    :ok
  end

  describe "database actions over tables" do
    test "it can list tables" do
      assert DB.table_names() == {:ok, []}
    end

    test "it can create a table" do
      {:ok, _} = DB.create_table("test", [%{some: :data}])
      assert DB.table_names() == {:ok, ["test"]}
    end

    test "it can create an empty table" do
      schema = Schema.from([utf8_fld("fooo")])
      {:ok, _} = DB.create_empty_table("empty", schema)
      assert DB.table_names() == {:ok, ["empty"]}
    end

    test "it can create a vector field" do
      text_field = utf8_fld("text_field")
      vec_field = vector_fld("vector", text_field)

      schema = Schema.from([text_field, vec_field])
      {:ok, _} = DB.create_empty_table("empty2", schema)
      assert DB.table_names() == {:ok, ["empty2"]}
    end

    defp utf8_fld(name) do
      %Field{name: name, field_type: %Utf8{}, nullable: false}
    end

    defp vector_fld(name, child, dimension \\ 384) do
      %Field{
        name: name,
        field_type: %FixedSizeList{
          dimension: dimension,
          child: child
        },
        nullable: false
      }
    end
  end
end
