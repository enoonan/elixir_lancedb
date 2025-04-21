defmodule ElixirLanceDB.Native.Table.MergeInsertConfig do
  defstruct on: [],
            when_matched_update_all: false,
            when_matched_update_all_filt: nil,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_delete_filt: nil

  @type t() :: %__MODULE__{
          on: list(String.t()),
          when_matched_update_all: boolean(),
          when_matched_update_all_filt: String.t() | nil,
          when_not_matched_insert_all: boolean(),
          when_not_matched_by_source_delete: boolean(),
          when_not_matched_by_source_delete_filt: String.t() | nil
        }
end
