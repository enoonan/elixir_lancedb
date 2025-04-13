defmodule ElixirLanceDB.IndexConfigTest do
  use ExUnit.Case
  alias ElixirLanceDB.Table.IndexConfig

  describe "Index Config ::" do
    test "it encodes to JSON" do
      result = IndexConfig.ivfPq("vector")

      assert result == %IndexConfig{
               field: "vector",
               config: %ElixirLanceDB.Table.IndexConfig.IvfPq{
                 distance_type: :l2,
                 num_partitions: nil,
                 num_subvectors: nil,
                 num_bits: 8
               }
             }

      result |> Jason.encode!()
    end
  end
end
