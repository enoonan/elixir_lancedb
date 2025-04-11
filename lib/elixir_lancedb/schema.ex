defmodule ElixirLanceDB.Schema do
  alias ElixirLanceDB.Schema.Field
  @derive Jason.Encoder
  defstruct ~w(fields)a

  @type t() :: %__MODULE__{
          fields: list(Field.t())
        }

  def from(fields) when is_list(fields) do
    %__MODULE__{
      fields: fields
    }
  end
end
