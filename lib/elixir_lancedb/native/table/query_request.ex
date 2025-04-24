defmodule ElixirLanceDB.Native.Table.QueryRequest do
  alias ElixirLancedb.Native.Table.FullTextSearchQueryRequest
  alias ElixirLancedb.Native.Table.FullTextSearchQuery
  alias ElixirLanceDB.Native.Table.QueryFilter

  defstruct [
    :filter,
    limit: nil,
    full_text_search: nil
    # :offset,
    # :select,
    # :fast_search,
    # :with_row_id,
    # :prefilter,
    # :reranker,
    # :norm
  ]

  @type t() :: %__MODULE__{
          filter: QueryFilter.t() | map() | nil,
          limit: integer() | nil,
          full_text_search: FullTextSearchQuery.t() | nil
          # offset: integer() | nil,
          # select: map() | nil,
          # fast_search: boolean(),
          # with_row_id: boolean(),
          # prefilter: boolean(),
          # reranker: map() | nil,
          # norm: map() | nil
        }
  def new(), do: %__MODULE__{}

  def limit(%__MODULE__{} = request, limit) do
    %__MODULE__{
      request
      | limit: limit
    }
  end

  def filter(%__MODULE__{} = request, sql, opts \\ []) when is_binary(sql) and is_list(opts) do
    %__MODULE__{
      request
      | filter: QueryFilter.new(sql, opts)
    }
  end

  def fts(%__MODULE__{} = request, query, opts \\ []) when is_binary(query) and is_list(opts) do
    %__MODULE__{
      request
      | full_text_search: FullTextSearchQueryRequest.new(query, opts)
    }
  end
end
