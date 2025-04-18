defmodule ElixirLanceDB.Native.Table.QueryFilter do
  defstruct [:sql, :substrait, :datafusion]

  @type t() :: %__MODULE__{
          sql: String.t() | nil,
          substrait: integer() | nil,
          datafusion: term() | nil
        }
end
