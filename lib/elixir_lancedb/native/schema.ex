defmodule ElixirLanceDB.Native.Schema do
  @derive Jason.Encoder
  defstruct ~w(fields metadata)a

  @type t() :: %__MODULE__{
          fields: list(atom() | tuple()),
          metadata: map()
        }

  def from(fields, metadata \\ %{}) when is_list(fields) when is_map(metadata) do
    %__MODULE__{
      metadata: metadata,
      fields: fields
    }
  end
end
