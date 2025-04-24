defmodule ElixirLanceDB.Native.Table.QueryFilter do
  defstruct [
    :sql,
    # :substrait,
    # :datafusion
  ]

  @type t() :: %__MODULE__{
          sql: String.t() | nil,
          # substrait: integer() | nil,
          # datafusion: term() | nil
        }

  def new(sql, opts \\ []) when is_binary(sql) and is_list(opts) do
    %__MODULE__{
      sql: sql
    }
  end
end
