defmodule ElixirLanceDB.Native.Table.VectorQueryRequest do
  alias ElixirLanceDB.Native.Table.QueryRequest

  defstruct [
    :base,
    postfilter: false,
    column: nil,
    query_vector: [],
    nprobes: 20,
    lower_bound: nil,
    upper_bound: nil,
    ef: nil,
    refine_factor: nil,
    distance_type: nil,
    use_index: true
  ]

  @type t() :: %__MODULE__{
          # base querying
          base: QueryRequest.t(),
          postfilter: boolean(),

          # Vector
          column: String.t() | nil,
          query_vector: list(float()),
          nprobes: non_neg_integer() | nil,
          lower_bound: float() | nil,
          upper_bound: float() | nil,
          ef: non_neg_integer() | nil,
          refine_factor: non_neg_integer() | nil,
          distance_type: atom() | nil,
          use_index: boolean()
        }

  def new(query_vector, opts \\ []) when is_list(query_vector) and is_list(opts) do
    base_request = Keyword.get(opts, :base, %QueryRequest{})

    %__MODULE__{
      base: base_request,
      query_vector: query_vector,
      column: Keyword.get(opts, :column, nil),
      postfilter: Keyword.get(opts, :postfilter, false),
      nprobes: Keyword.get(opts, :nprobes, 20),
      lower_bound: Keyword.get(opts, :lower_bound, nil),
      upper_bound: Keyword.get(opts, :upper_bound, nil),
      ef: Keyword.get(opts, :ef, nil),
      refine_factor: Keyword.get(opts, :refine_factor, nil),
      distance_type: Keyword.get(opts, :distance_type, nil),
      use_index: Keyword.get(opts, :use_index, true)
    }
  end

  def with_vector(%__MODULE__{} = req, query_vector) when is_list(query_vector) do
    %__MODULE__{
      req
      | query_vector: query_vector
    }
  end

  def filter(%__MODULE__{} = req, filter, opts \\ []) when is_binary(filter) and is_list(opts) do
    %__MODULE__{
      req
      | base: req.base |> QueryRequest.filter(filter, opts)
    }
  end

  def hybridize(%__MODULE__{} = req, query, opts \\ []) do
    %__MODULE__{
      req
      | base: req.base |> QueryRequest.fts(query, opts)
    }
  end
end
