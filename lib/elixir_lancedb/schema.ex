defmodule ElixirLanceDB.Schema do
  alias ElixirLanceDB.Schema.Field
  @derive Jason.Encoder
  defstruct ~w(fields metadata)a

  @type t() :: %__MODULE__{
          fields: list(Field.t()) | list(term()),
          metadata: map()
        }

  def from(fields, metadata \\ %{}) when is_list(fields) when is_map(metadata) do
    %__MODULE__{
      fields: fields,
      metadata: metadata
    }
  end

end
