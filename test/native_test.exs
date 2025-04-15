defmodule ElixirLanceDB.NativeTest do
  use ExUnit.Case
  alias ElixirLanceDB.Native

  describe "Native :: Connection" do
    test "it returns a connection" do
      assert Native.connect(data_uri()) |> is_reference()
    end

    test "it shows empty table names list" do
      conn = Native.connect(data_uri())
      result = Native.table_names(conn)
      assert result |> is_list()
      assert result |> Enum.empty?()
    end
  end

  defp data_uri(), do: Path.join(File.cwd!(), "data/testing")
end
