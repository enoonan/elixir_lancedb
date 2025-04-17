defmodule ElixirLanceDB.Native.SchemaTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native.Schema.Field
  alias ElixirLanceDB.Native.Schema

  describe "Schema :: Inference ::" do
    test "it takes an empty map and return a schema with no fields" do
      schema = infer()
      assert schema.fields |> is_list()
      assert schema.fields |> Enum.empty?()
      assert schema.metadata |> is_map()
    end

    test "it can infer a utf8 field" do
      schema = infer(%{foo: "bar"})

      assert schema.fields == [
               %Field{
                 name: "foo",
                 field_type: :utf8,
                 nullable: false
               }
             ]
    end

    test "it can infer list" do
      {:ok, schema} = Schema.infer([%{foo: [1.0, 2.0]}, %{foo: [3.0, 4.0]}])

      assert schema.fields == [
               %Field{
                 name: "foo",
                 field_type: {:list, :float32},
                 nullable: false
               }
             ]
    end

    test "it can upgrade list to fixed_size_list" do
      {:ok, schema} =
        1..150
        |> Enum.to_list()
        |> Enum.map(fn _ -> %{foo: [:rand.uniform(), :rand.uniform(), :rand.uniform()]} end)
        |> Schema.infer()

      assert schema.fields == [
               %ElixirLanceDB.Native.Schema.Field{
                 name: "foo",
                 field_type: {:fixed_size_list, :float32, 3},
                 nullable: false
               }
             ]
    end

    test "it won't upgrade list with too small of a sample site" do
      {:ok, schema} =
        1..99
        |> Enum.to_list()
        |> Enum.map(fn _ -> %{foo: [:rand.uniform(), :rand.uniform(), :rand.uniform()]} end)
        |> Schema.infer()

      assert schema.fields == [
               %ElixirLanceDB.Native.Schema.Field{
                 name: "foo",
                 field_type: {:list, :float32},
                 nullable: false
               }
             ]
    end

    test "it won't upgrade list with inconsistent list lengths" do
      data =
        1..150
        |> Enum.to_list()
        |> Enum.map(fn _ -> %{foo: [:rand.uniform(), :rand.uniform(), :rand.uniform()]} end)

      data = [%{foo: [:rand.uniform(), :rand.uniform()]}] ++ data

      {:ok, schema} = Schema.infer(data)

      assert schema.fields == [
               %ElixirLanceDB.Native.Schema.Field{
                 name: "foo",
                 field_type: {:list, :float32},
                 nullable: false
               }
             ]
    end
  end

  defp infer(sample \\ %{}) do
    {:ok, %Schema{} = schema} = Schema.infer([sample])
    schema
  end
end
