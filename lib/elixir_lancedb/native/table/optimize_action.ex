defmodule ElixirLanceDB.Native.Table.OptimizeAction do
  defmodule All do
    defstruct action_type: :all
    @type t() :: %__MODULE__{action_type: :all}
  end
end
