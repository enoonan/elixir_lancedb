defmodule ElixirNativeDB.Native.InferTypeTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native.Schema.Infer

  describe "Infer Type :: " do
    test "it can infer primitive types" do
      assert Infer.type(nil) == :null
      assert Infer.type("foo") == :utf8
      assert Infer.type(1) == :int32
      assert Infer.type(3.14) == :float32
    end

    test "it can infer list types" do
      assert Infer.type([nil]) == {:list, :null}
      assert Infer.type(["foo"]) == {:list, :utf8}
      assert Infer.type([1]) == {:list, :int32}
      assert Infer.type([1.0, 2.0, 3.0]) == {:list, :float32}
    end

    test "it can infer dates and time" do
      date = Date.new!(2000, 1, 1)
      time = Time.new!(1, 1, 1)

      assert Infer.type(date) == :date
      assert Infer.type(time) == :time
      {t, ms, _tz} = Infer.type(DateTime.new!(date, time))
      assert {t, ms} == {:datetime, :microsecond}
      assert Infer.type(NaiveDateTime.new!(date, time)) == {:naive_datetime, :microsecond}
    end

    test "it can infer maps" do
      result =
        Infer.type(%{
          e: [%{foo: 3.12}],
          d: "bar",
          c: %{key_str: "foooo"},
          b: 3,
          a: ["1", "2"]
        })

      assert result ==
               {:struct,
                [
                  {"a", {:list, :utf8}},
                  {"b", :int32},
                  {"c", {:struct, [{"key_str", :utf8}]}},
                  {"d", :utf8},
                  {"e", {:list, {:struct, [{"foo", :float32}]}}}
                ]}
    end
  end
end
