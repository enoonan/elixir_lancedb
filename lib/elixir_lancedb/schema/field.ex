defmodule ElixirLanceDB.Schema.Field do
  @derive Jason.Encoder
  defstruct ~w(name field_type nullable)a

  @type t() :: %__MODULE__{
          name: String.t(),
          field_type: struct(),
          nullable: boolean()
        }
end
