defmodule ElixirLanceDB.Native.Table.Index do
  alias ElixirLanceDB.Native.Table.Index.FTS.TokenizerConfig
  alias ElixirLanceDB.Native.Table.Index.IvfPq

  defmodule Auto do
    defstruct index_type: :auto

    @type t() :: %__MODULE__{index_type: :auto}
  end

  def auto() do
    %Auto{}
  end

  defmodule BTree do
    defstruct index_type: :btree

    @type t() :: %__MODULE__{index_type: :btree}
  end

  def btree() do
    %BTree{}
  end

  defmodule Bitmap do
    defstruct index_type: :bitmap

    @type t() :: %__MODULE__{index_type: :bitmap}
  end

  def bitmap() do
    %Bitmap{}
  end

  defmodule LabelList do
    defstruct index_type: :label_list

    @type t() :: %__MODULE__{index_type: :label_list}
  end

  def label_list() do
    %LabelList{}
  end

  defmodule FTS do
    defstruct [:tokenizer_configs, index_type: :fts, with_position: true]

    @type t() :: %__MODULE__{
            index_type: :fts,
            with_position: boolean(),
            tokenizer_configs: TokenizerConfig.t()
          }

    defmodule TokenizerConfig do
      defstruct base_tokenizer: :simple,
                language: :english,
                max_token_length: 40,
                lower_case: true,
                stem: false,
                remove_stop_words: false,
                ascii_folding: false

      @type t() :: %__MODULE__{
              base_tokenizer: :simple | :whitespace | :raw,
              language:
                :arabic
                | :danish
                | :dutch
                | :english
                | :finnish
                | :french
                | :german
                | :greek
                | :hungarian
                | :italian
                | :norwegian
                | :portuguese
                | :romanian
                | :russian
                | :spanish
                | :swedish
                | :tamil
                | :turkish,
              max_token_length: integer() | nil,
              lower_case: boolean(),
              stem: boolean(),
              remove_stop_words: boolean(),
              ascii_folding: boolean()
            }
    end
  end

  def fts(config \\ []) do
    %FTS{
      with_position: get(config, :with_position, true),
      tokenizer_configs: %TokenizerConfig{
        base_tokenizer: get(config, :base_tokenizer, :simple),
        language: get(config, :language, :english),
        max_token_length: get(config, :max_token_length, 40),
        lower_case: get(config, :lower_case, true),
        stem: get(config, :stem, false),
        remove_stop_words: get(config, :remove_stop_words, false),
        ascii_folding: get(config, :ascii_folding, false)
      }
    }
  end

  @type distance_type() :: :l2 | :cosine | :dot | :hamming

  defmodule IvfFlat do
    defstruct index_type: :ifv_flat,
              distance_index_type: :l2,
              num_partitions: nil,
              sample_rate: 256,
              max_iterations: 50

    @type t() :: %__MODULE__{
            index_type: :ifv_flat,
            distance_index_type: ElixirLanceDB.Native.Table.IndexConfig.distance_type(),
            num_partitions: integer() | nil,
            sample_rate: integer(),
            max_iterations: integer()
          }
  end

  def ivf_flat(config \\ []) do
    %IvfFlat{
      distance_index_type: get(config, :distance_type, :l2),
      num_partitions: get(config, :num_partitions),
      sample_rate: get(config, :sample_rate, 256),
      max_iterations: get(config, :max_iterations, 50)
    }
  end

  defmodule IvfPq do
    defstruct index_type: :ivf_pq,
              distance_index_type: :l2,
              num_partitions: nil,
              sample_rate: 256,
              max_iterations: 50,
              num_subvectors: nil,
              num_bits: nil

    @type t() :: %__MODULE__{
            index_type: :ivf_pq,
            distance_index_type: ElixirLanceDB.Native.Table.IndexConfig.distance_type(),
            num_partitions: integer() | nil,
            sample_rate: integer(),
            max_iterations: integer(),
            num_subvectors: integer() | nil,
            num_bits: integer() | nil
          }
  end

  def ivf_pq(config \\ []) do
    %IvfPq{
      index_type: :ivf_pq,
      distance_index_type: get(config, :distance_type, :l2),
      sample_rate: get(config, :sample_rate, 256),
      max_iterations: get(config, :max_iterations, 50),
      num_partitions: get(config, :num_partitions),
      num_subvectors: get(config, :num_subvectors),
      num_bits: get(config, :num_bits)
    }
  end

  defmodule IvfHnswPq do
    defstruct index_type: :ivf_hnsw_pq,
              distance_index_type: :l2,
              num_partitions: nil,
              sample_rate: 256,
              max_iterations: 50,
              m: 20,
              ef_construction: 300,
              num_sub_vectors: nil,
              num_bits: nil

    @type t() :: %__MODULE__{
            index_type: :ivf_hnsw_pq,
            distance_index_type: ElixirLanceDB.Native.Table.IndexConfig.distance_type(),
            num_partitions: integer() | nil,
            sample_rate: integer(),
            max_iterations: integer(),
            m: integer(),
            ef_construction: integer(),
            num_sub_vectors: integer() | nil,
            num_bits: integer() | nil
          }
  end

  def ivf_hnsw_pq(config \\ []) do
    %IvfHnswPq{
      index_type: :ivf_hnsw_pq,
      distance_index_type: get(config, :distance_type, :l2),
      num_partitions: get(config, :num_partitions),
      sample_rate: get(config, :sample_rate, 256),
      max_iterations: get(config, :max_iterations, 50),
      m: get(config, :m, 20),
      ef_construction: get(config, :ef_construction, 300),
      num_sub_vectors: get(config, :num_sub_vectors),
      num_bits: get(config, :num_bits)
    }
  end

  defmodule IvfHnswSq do
    defstruct index_type: :ivf_hnsw_sq,
              distance_index_type: :l2,
              num_partitions: nil,
              sample_rate: 256,
              max_iterations: 50,
              m: 20,
              ef_construction: 300

    @type t() :: %__MODULE__{
            index_type: :ivf_hnsw_sq,
            distance_index_type: ElixirLanceDB.Native.Table.IndexConfig.distance_type(),
            num_partitions: integer() | nil,
            sample_rate: integer(),
            max_iterations: integer(),
            m: integer(),
            ef_construction: integer()
          }
  end

  def ivf_hnsw_sq(config \\ []) do
    %IvfHnswSq{
      distance_index_type: get(config, :distance_type, :l2),
      num_partitions: get(config, :num_partitions),
      sample_rate: get(config, :sample_rate, 256),
      max_iterations: get(config, :max_iterations, 50),
      m: get(config, :m, 20),
      ef_construction: get(config, :ef_construction, 300)
    }
  end

  defp get(list, key, default \\ nil) do
    list |> Keyword.get(key, default)
  end
end
