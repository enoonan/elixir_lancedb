defmodule ElixirLanceDB.Native.Schema.Field do
  @derive Jason.Encoder
  defstruct ~w(name field_type nullable)a

  @type t() :: %__MODULE__{
          name: String.t(),
          field_type: atom() | tuple(),
          nullable: boolean()
        }

  def from_inferred_type({name, {:list, child_type}}) do
    %__MODULE__{
      name: name,
      field_type: {:list, from_inferred_type({"item", child_type})},
      nullable: true
    }
  end

  def from_inferred_type({name, {:fixed_size_list, child_type, dimension}}) do
    %__MODULE__{
      name: name,
      field_type: {:list, from_inferred_type({"item", child_type}), dimension},
      nullable: true
    }
  end

  def from_inferred_type({name, type})
      when is_binary(name) and (is_atom(type) or is_tuple(type)) do
    %__MODULE__{
      name: name,
      field_type: type,
      nullable: true
    }
  end

  def utf8(name, nullable \\ true) do
    %__MODULE__{
      name: name,
      field_type: :utf8,
      nullable: nullable
    }
  end

  def float32(name, nullable \\ true) do
    %__MODULE__{
      name: name,
      field_type: :float32,
      nullable: nullable
    }
  end

  def int32(name, nullable \\ true) do
    %__MODULE__{
      name: name,
      field_type: :int32,
      nullable: nullable
    }
  end

  def list(name, %__MODULE__{} = child, nullable \\ true) do
    %__MODULE__{
      name: name,
      field_type: {:list, child},
      nullable: nullable
    }
  end

  def vector(name, %__MODULE__{} = child, dimension), do: fixed_size_list(name, child, dimension)

  def fixed_size_list(name, %__MODULE__{} = child, dimension) when is_integer(dimension) do
    %__MODULE__{
      name: name,
      field_type: {:fixed_size_list, child, dimension},
      nullable: true
    }
  end
end
