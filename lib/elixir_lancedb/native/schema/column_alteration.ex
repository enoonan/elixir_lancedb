defmodule ElixirLanceDB.Native.Schema.ColumnAlteration do
  defstruct [:path, rename: nil, nullable: nil, data_type: nil]

  @type t() :: %__MODULE__{
          path: String.t(),
          rename: String.t() | nil,
          nullable: boolean() | nil,
          data_type: atom() | tuple() | nil
        }

  def new(path, opts \\ []) when is_binary(path) do
    %__MODULE__{
      path: path,
      rename: Keyword.get(opts, :rename, nil),
      nullable: Keyword.get(opts, :nullable, nil),
      data_type: Keyword.get(opts, :data_type, nil)
    }
  end
end
