defmodule ElixirLanceDB.Schema.Field do
  @derive Jason.Encoder
  defstruct ~w(name field_type nullable)a

  alias ElixirLanceDB.Schema.FieldTypes.{Utf8, Float, FixedSizeList}

  @type t() :: %__MODULE__{
          name: String.t(),
          field_type: struct(),
          nullable: boolean()
        }

  def utf8(name, nullable \\ false) do
    %__MODULE__{
      name: name,
      field_type: %Utf8{},
      nullable: nullable
    }
  end

  def float(name, precision, nullable \\ false) do
    %__MODULE__{
      name: name,
      field_type: %Float{
        precision: precision
      },
      nullable: nullable
    }
  end

  def vector(name, %__MODULE__{} = child, dimension) do
    %__MODULE__{
      name: name,
      field_type: %FixedSizeList{
        dimension: dimension,
        child: child
      },
      nullable: true
    }
  end
end
