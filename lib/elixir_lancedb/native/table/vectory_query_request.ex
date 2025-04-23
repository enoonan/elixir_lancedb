defmodule ElixirLanceDB.Native.Table.VectorQueryRequest do
  defstruct limit: nil,
            filter: nil,
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

  @type t() :: %__MODULE__{
          # base querying
          limit: non_neg_integer() | nil,
          filter: String.t() | nil,
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

  def new(), do: %__MODULE__{}

  def with_vector(%__MODULE__{} = req, query_vector) when is_list(query_vector) do
    %__MODULE__{
      req
      | query_vector: query_vector
    }
  end
end
