defmodule ElixirLanceDB.Native.Table.QueryRequest do
  # alias ElixirLanceDB.Native.Table.QueryFilter

  defstruct [
    :limit
    # :offset,
    # :filter,
    # :full_text_search,
    # :select,
    # :fast_search,
    # :with_row_id,
    # :prefilter,
    # :reranker,
    # :norm
  ]

  @type t() :: %__MODULE__{
          limit: integer() | nil
          # offset: integer() | nil,
          # filter: QueryFilter.t() | nil,
          # full_text_search: map() | nil,
          # select: map() | nil,
          # fast_search: boolean(),
          # with_row_id: boolean(),
          # prefilter: boolean(),
          # reranker: map() | nil,
          # norm: map() | nil
        }
end
