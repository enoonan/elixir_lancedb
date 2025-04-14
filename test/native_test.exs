defmodule ElixirLanceDB.NativeTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native

  describe "Native :: Connection" do
    test "it returns a connection" do
      ref = Native.connect("./data");
      assert is_reference(ref)
    end
  end
end
