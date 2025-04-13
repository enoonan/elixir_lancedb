defmodule ElixirLanceDB.Table.IndexConfig do
  alias ElixirLanceDB.Table.IndexConfig
  alias ElixirLanceDB.Table.IndexConfig.IvfPq
  defstruct [:field, :config]

  defmodule IvfPq do
    defstruct distance_type: :l2, num_partitions: nil, num_subvectors: nil, num_bits: 8

    @type t() :: %__MODULE__{
            distance_type: :l2 | :cosine | :dot,
            num_partitions: integer() | nil,
            num_subvectors: integer() | nil,
            num_bits: 4 | 8
          }
  end

  @type t() :: %__MODULE__{
          field: String.t(),
          config: IvfPq.t()
        }

  def ivfPq(field, config \\ []) do
    %IndexConfig{
      field: field,
      config: %IvfPq{
        distance_type: get(config, :distance_type, :l2),
        num_partitions: get(config, :num_partitions),
        num_subvectors: get(config, :num_subvectors),
        num_bits: get(config, :num_bits, 8)
      }
    }
  end

  defp get(list, key, default \\ nil) do
    list |> Keyword.get(key, default)
  end
end

defimpl Jason.Encoder, for: ElixirLanceDB.Table.IndexConfig do
  alias ElixirLanceDB.Table.IndexConfig

  def encode(%IndexConfig{field: field, config: config}, opts) do
    %{field: field, config: config} |> Jason.Encode.map(opts)
  end
end

defimpl Jason.Encoder, for: ElixirLanceDB.Table.IndexConfig.IvfPq do
  alias ElixirLanceDB.Table.IndexConfig.IvfPq

  def encode(%IvfPq{} = val, opts) do
    val
    |> Map.from_struct()
    |> Enum.filter(fn {_, v} -> v != nil end)
    |> Enum.into(%{})
    |> Jason.Encode.map(opts)
  end
end
