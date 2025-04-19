defmodule ElixirLanceDB.Native.Table.QueryRequest do
  alias ElixirLanceDB.Native.Table.QueryFilter

  defstruct [
    :filter,
    :limit
    # :offset,
    # :full_text_search,
    # :select,
    # :fast_search,
    # :with_row_id,
    # :prefilter,
    # :reranker,
    # :norm
  ]

  @type t() :: %__MODULE__{
          filter: QueryFilter.t() | map() | nil,
          limit: integer() | nil
          # offset: integer() | nil,
          # full_text_search: map() | nil,
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

  def filter_sql(%__MODULE__{} = request, sql) when is_binary(sql) do
    %__MODULE__{
      request |
      filter: %QueryFilter{
        sql: sql
      }
    }
  end
end
