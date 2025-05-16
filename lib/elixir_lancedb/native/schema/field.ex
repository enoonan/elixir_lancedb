defmodule ElixirLanceDB.Native.Schema.Field do
  @derive Jason.Encoder
  defstruct ~w(name field_type nullable)a

  @type t() :: %__MODULE__{
          name: String.t(),
          field_type: atom() | tuple(),
          nullable: boolean()
        }

  def new(name, field_type, opts) do
    %__MODULE__{
      name: name,
      field_type: field_type,
      nullable: Keyword.get(opts, :nullable, true)
    }
  end

  def from_inferred_type({name, {:list, child_type}}) do
    new(name, {:list, from_inferred_type({"item", child_type})}, nullable: true)
  end

  def from_inferred_type({name, {:fixed_size_list, child_type, dimension}}) do
    new(name, {:list, from_inferred_type({"item", child_type}), dimension}, nullable: true)
  end

  def from_inferred_type({name, type})
      when is_binary(name) and (is_atom(type) or is_tuple(type)) do
    new(name, type, nullable: true)
  end

  def boolean(name, opts \\ []) do
    new(name, :boolean, opts)
  end

  def utf8(name, opts \\ []) do
    new(name, :utf8, opts)
  end

  def float32(name, opts \\ []) do
    new(name, :float32, opts)
  end

  def int32(name, opts \\ []) do
    new(name, :int32, opts)
  end

  def list(name, %__MODULE__{} = child, opts \\ []) do
    new(name, {:list, child}, opts)
  end

  def vector(name, %__MODULE__{} = child, dimension, opts \\ []),
    do: fixed_size_list(name, child, dimension, opts)

  def fixed_size_list(name, %__MODULE__{} = child, dimension, opts \\ [])
      when is_integer(dimension) do
    new(name, {:fixed_size_list, child, dimension}, opts)
  end

  def date32(name, opts \\ []) do
    new(name, :date32, opts)
  end

  def date64(name, opts \\ []) do
    new(name, :date64, opts)
  end
end
