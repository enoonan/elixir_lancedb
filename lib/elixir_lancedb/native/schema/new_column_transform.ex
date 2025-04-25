defmodule ElixirLancedb.Native.Schema.NewColumnTransform do
  alias ElixirLancedb.Native.Schema.NewColumnTransform.AllNulls
  alias ElixirLanceDB.Native.Schema

  defmodule AllNulls do
    alias ElixirLanceDB.Native.Schema
    defstruct [:schema, transform_type: :all_nulls]

    @type t() :: %__MODULE__{
            transform_type: :all_nulls,
            schema: Schema.t()
          }
  end

  def all_nulls(fields, %{} = metadata \\ %{}) when is_list(fields) do
    %AllNulls{
      schema: Schema.from(fields, metadata)
    }
  end
end
