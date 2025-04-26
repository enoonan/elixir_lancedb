defmodule ElixirLanceDB.Native.Table.FullTextSearchQueryRequest do
  defstruct [:query, limit: nil, columns: [], wand_factor: nil]

  @type t() :: %__MODULE__{
          query: String.t(),
          limit: integer() | nil,
          columns: list(String.t()),
          wand_factor: float() | nil
        }

  def new(query, opts \\ []) when is_binary(query) and is_list(opts) do
    %__MODULE__{
      query: query,
      limit: Keyword.get(opts, :limit, nil),
      columns: Keyword.get(opts, :columns, []),
      wand_factor: Keyword.get(opts, :wand_factor, nil)
    }
  end
end
