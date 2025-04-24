defmodule ElixirLancedb.Native.Table.OptimizeAction do
  defmodule ElixirLancedb.Native.Table.OptimizeAction.All do
    defstruct [action_type: :all]
    @type t() :: %__MODULE__{action_type: :all}
  end
end
