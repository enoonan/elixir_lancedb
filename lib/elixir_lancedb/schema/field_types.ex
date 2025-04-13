defmodule ElixirLanceDB.Schema.FieldTypes do
  alias ElixirLanceDB.Schema.FieldTypes.{Utf8, FixedSizeList, List, Float32, Int32}

  def valid_types() do
    [Utf8, FixedSizeList, List, Float32, Int32]
  end

  def is_field_type?(thing) do
    valid_types() |> Enum.any?(fn type -> thing |> is_struct(type) end)
  end

  defmodule Utf8 do
    defstruct []
    @type t() :: %__MODULE__{}
  end

  defmodule FixedSizeList do
    defstruct ~w(dimension child)a

    @type t() :: %__MODULE__{
            dimension: non_neg_integer(),
            child: Field.t()
          }
  end

  defmodule List do
    defstruct ~w(child)a

    @type t() :: %__MODULE__{
            child: Field.t()
          }
  end

  defmodule Float do
    defstruct [:precision]

    @type t() :: %__MODULE__{
            precision: integer()
          }
  end

  defmodule Float32 do
    defstruct []
  end

  defmodule Int32 do
    defstruct []
  end
end

defimpl Jason.Encoder, for: ElixirLanceDB.Schema.FieldTypes.Utf8 do
  alias ElixirLanceDB.Schema.FieldTypes.Utf8

  def encode(%Utf8{}, opts) do
    %{type: :utf8} |> Jason.Encode.map(opts)
  end
end

defimpl Jason.Encoder,
  for: ElixirLanceDB.Schema.FieldTypes.FixedSizeList do
  alias ElixirLanceDB.Schema.FieldTypes.FixedSizeList

  def encode(%FixedSizeList{child: child, dimension: dimension}, opts) do
    %{type: :fixed_size_list, child: child, dimension: dimension}
    |> Jason.Encode.map(opts)
  end
end

defimpl Jason.Encoder, for: ElixirLanceDB.Schema.FieldTypes.List do
  alias ElixirLanceDB.Schema.FieldTypes.List

  def encode(%List{child: child}, opts) do
    %{type: :list, child: child} |> Jason.Encode.map(opts)
  end
end

defimpl Jason.Encoder,
  for: ElixirLanceDB.Schema.FieldTypes.Float32 do
  alias ElixirLanceDB.Schema.FieldTypes.Float32

  def encode(%Float32{} = _, opts) do
    %{type: :float32} |> Jason.Encode.map(opts)
  end
end

defimpl Jason.Encoder,
  for: ElixirLanceDB.Schema.FieldTypes.Float do
  alias ElixirLanceDB.Schema.FieldTypes.Float

  def encode(%Float{precision: precision} = _, opts) do
    %{type: :float32, precision: precision} |> Jason.Encode.map(opts)
  end
end

defimpl Jason.Encoder, for: ElixirLanceDB.Schema.FieldTypes.Int32 do
  alias ElixirLanceDB.Schema.FieldTypes.Int32

  def encode(%Int32{} = _, opts) do
    %{type: :int32} |> Jason.Encode.map(opts)
  end
end
