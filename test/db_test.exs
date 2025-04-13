defmodule ElixirLanceDB.DbTest do
  use ExUnit.Case
  alias ElixirLanceDB.Schema.Field
  alias ElixirLanceDB.{Schema, DB}

  setup do
    DB.drop_all_tables()
    :ok
  end

  describe "database tables ::" do
    test "it can list tables" do
      assert DB.table_names() == {:ok, []}
    end

    test "it can create a table" do
      {:ok, _} = DB.create_table("test", [%{some: :data}])
      assert DB.table_names() == {:ok, ["test"]}
    end

    test "it can create an empty table" do
      schema = Schema.from([Field.utf8("fooo")])
      {:ok, _} = DB.create_empty_table("empty", schema)
      assert DB.table_names() == {:ok, ["empty"]}
    end

    test "it can create a vector field" do
      text_field = Field.utf8("text_field")
      vec_field = Field.vector("vector", text_field, 384)

      schema = Schema.from([text_field, vec_field])
      {:ok, _} = DB.create_empty_table("empty2", schema)
      assert DB.table_names() == {:ok, ["empty2"]}
    end
  end
end
