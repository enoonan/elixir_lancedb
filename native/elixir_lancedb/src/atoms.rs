rustler::atoms! {
    // Results and errors (mostly Lance error types)
    ok,
    error,
    invalid_table_name,
    name,
    reason,
    invalid_input,
    message,
    table_not_found,
    unknown,
    database_not_found,
    database_already_exists,
    index_not_found,
    embedding_function_not_found,
    table_already_exists,
    create_dir,
    schema,
    runtime,
    object_store,
    lance,
    arrow,
    not_supported,
    path,
    source,
    other,

    // Rustler-specific errors
    rustler_bad_arg,
    rustler_atom,
    rustler_raise_atom,
    rustler_raise_term,
    rustler_term,

    // decoding atoms
    filter,
    sql,
    limit,

    // datatypes
    s,
    f,
    u,
    utf8,
    timestamp_us,
    timestamp_ns,
    date,
    unsupported_type,

// deprecating
    tables_dropped,
    table_dropped,
    created_table
}
