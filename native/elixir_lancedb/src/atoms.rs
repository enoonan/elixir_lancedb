rustler::atoms! {
    // Results and errors (mostly Lance error types)
    ok,
    error,
    name,
    reason,
    invalid_input,
    db_connection_closed,
    db_mutex_lock_poisoned,
    table_connection_closed,
    table_mutex_lock_poisoned,

    // Lance Errors
    lance_invalid_table_name,
    lance_invalid_input,
    lance_message,
    lance_table_not_found,
    lance_unknown,
    lance_database_not_found,
    lance_database_already_exists,
    lance_index_not_found,
    lance_embedding_function_not_found,
    lance_table_already_exists,
    lance_create_dir,
    lance_schema,
    lance_runtime,
    lance_object_store,
    lance,
    lance_arrow,
    lance_not_supported,
    lance_path,
    lance_source,
    lance_timeout,
    lance_other,

    // Rustler-specific errors
    rustler_bad_arg,
    rustler_atom,
    rustler_raise_atom,
    rustler_raise_term,
    rustler_term,

    // Arrow-specific errors
    arrow_not_yet_implemented,
    arrow_external_error,
    arrow_cast_error,
    arrow_memory_error,
    arrow_parse_error,
    arrow_schema_error,
    arrow_compute_error,
    arrow_divide_by_zero,
    arrow_arithmetic_overflow,
    arrow_csv_error,
    arrow_json_error,
    arrow_io_error,
    arrow_ipc_error,
    arrow_invalid_argument_error,
    arrow_parquet_error,
    arrow_cdata_interface,
    arrow_dictionary_key_overflow_error,
    arrow_run_end_index_overflow_error,

    // table operations
    schema,
    filter,
    sql,
    limit,
    columns,
    query_vector,
    full_text_search,

    // optimize
    action_type,
    compaction,
    fragments_removed,
    fragments_added,
    files_removed,
    files_added,

    prune,
    bytes_removed,
    old_versions,

    // vector search
    base,
    column,
    nprobes,
    lower_bound,
    upper_bound,
    ef,
    refine_factor,
    distance_type,
    use_index,
    postfilter,

    // full text search
    query,
    wand_factor,

    // Index Config
    config,
    field,

    // table merge insert config
    on,
    when_matched_update_all,
    when_matched_update_all_filt,
    when_not_matched_insert_all,
    when_not_matched_by_source_delete,
    when_not_matched_by_source_delete_filt,

    // Index Type
    index_type,
    ivf_flat,
    ivf_pq,
    ivf_hnsw_pq,
    ivf_hnsw_sq,
    btree,
    bitmap,
    label_list,
    fts,
    sample_rate,
    distance_index_type,
    num_partitions,
    max_iterations,
    num_subvectors,
    num_bits,

    // fts index and tokenizer params
    with_position,
    tokenizer_configs,
    base_tokenizer,
    language,
    max_token_length,
    lower_case,
    stem,
    remove_stop_words,
    ascii_folding,

    // adding columns
    transform_type,

    // datatypes
    // s,
    // f,
    // u,
    // utf8,
    // timestamp_us,
    // timestamp_ns,
    // date,
    // unsupported_type,
}
