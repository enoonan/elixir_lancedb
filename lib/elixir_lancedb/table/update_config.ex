defmodule ElixirLanceDB.Native.Table.UpdateConfig do
  defstruct [:filter, columns: []]

  @type t() :: %__MODULE__{
          columns: list(ColumnOperation.t()),
          filter: String.t() | nil
        }

  defmodule ColumnOperation do
    defstruct [:column, :operation]

    @type t() :: %__MODULE__{
            column: String.t(),
            operation: String.t()
          }
  end


  def new(), do: %__MODULE__{}

  def column(%__MODULE__{} = cfg, name, operation)
      when is_binary(name) and is_binary(operation) do
    %__MODULE__{
      cfg
      | columns: [%ColumnOperation{column: name, operation: operation}] ++ cfg.columns
    }
  end

  def filter(%__MODULE__{filter: nil} = cfg, filter) when is_binary(filter) do
    %__MODULE__{
      cfg
      | filter: filter
    }
  end

  def filter(%__MODULE__{}, _),
    do:
      raise(
        "Cannot add more than one filter to ElixirLanceDB.Table.UpdateConfig. Use \"AND\" and \"OR\" in a single filter instead"
      )
end
