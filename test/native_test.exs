defmodule ElixirLanceDB.NativeTest do
  use ExUnit.Case

  describe "Native :: Connection" do
    test "it returns a connection" do
      assert ElixirLanceDB.Native.connect("./data") |> is_reference()
    end
  end
end
