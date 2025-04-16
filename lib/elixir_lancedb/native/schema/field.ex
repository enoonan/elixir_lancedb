defmodule ElixirLanceDB.Native.Schema.Field do
  @derive Jason.Encoder
  defstruct ~w(name field_type nullable)a

  @type t() :: %__MODULE__{
          name: String.t(),
          field_type: atom() | tuple(),
          nullable: boolean()
        }

  def utf8(name, nullable \\ false) do
    %__MODULE__{
      name: name,
      field_type: :utf8,
      nullable: nullable
    }
  end

  def float32(name, nullable \\ false) do
    %__MODULE__{
      name: name,
      field_type: :float32,
      nullable: nullable
    }
  end

  def list(name, %__MODULE__{} = child, nullable \\ false) do
    %__MODULE__{
      name: name,
      field_type: {:list, child},
      nullable: nullable
    }
  end

  def fixed_size_list(name, %__MODULE__{} = child, dimension), do: vector(name, child, dimension)

  def vector(name, %__MODULE__{} = child, dimension) when is_integer(dimension) do
    %__MODULE__{
      name: name,
      field_type: {:fixed_size_list, child, dimension},
      nullable: true
    }
  end
end
