use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
    schema,
    table::TableResource, term_to_arrow,
};
use arrow_array::{RecordBatch, RecordBatchIterator};

use lancedb::{Connection, Table};
use rustler::{Atom, ResourceArc, Term};

use std::sync::{Arc, Mutex};
pub struct DbConnResource(pub Arc<Mutex<Connection>>);

#[rustler::nif(schedule = "DirtyCpu")]
fn connect(uri: String) -> Result<ResourceArc<DbConnResource>> {
    let result = get_runtime().block_on(async { lancedb::connect(&uri).execute().await });

    match result {
        Ok(conn) => Ok(ResourceArc::new(DbConnResource(Arc::new(Mutex::new(conn))))),
        Err(err) => Err(Error::from(err)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_names(conn: ResourceArc<DbConnResource>) -> Result<Vec<String>> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.table_names().execute().await });

    match result {
        Ok(names) => Ok(names),
        Err(err) => Err(Error::from(err)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_all_tables(conn: ResourceArc<DbConnResource>) -> Result<()> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.drop_all_tables().await });

    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::from(err)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_table(conn: ResourceArc<DbConnResource>, table_name: String) -> Result<Atom> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.drop_table(&table_name).await });

    match result {
        Ok(_) => Ok(atoms::tables_dropped()),
        Err(err) => Err(Error::from(err)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn create_empty_table(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    schema: schema::Schema,
) -> Result<()> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async {
        conn.create_empty_table(table_name, Arc::new(schema.into_arrow()))
            .execute()
            .await
    });

    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::from(err)),
    }
}
#[rustler::nif(schedule = "DirtyCpu")]
fn create_table_with_data(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    erl_data: Term,
    erl_schema: schema::Schema,
) -> Result<()> {
    let arrow_schema = Arc::new(erl_schema.clone().into_arrow());
    let columnar_data = term_to_arrow::to_arrow(erl_data, erl_schema.clone())?;
    let batch = RecordBatchIterator::new(
        vec![RecordBatch::try_new(arrow_schema.clone(), columnar_data)],
        arrow_schema.clone(),
    );

    let conn = db_conn(conn);
    let result = get_runtime().block_on(async {
        conn.create_table(table_name, Box::new(batch))
            .execute()
            .await
    });
    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::from(err)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn open_table(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
) -> Result<ResourceArc<TableResource>> {
    let conn = db_conn(conn);
    let result: Result<Table> = get_runtime().block_on(async {
        conn.open_table(table_name)
            .execute()
            .await
            .map_err(|e| Error::from(e))
    });

    match result {
        Ok(table) => Ok(ResourceArc::new(TableResource(Arc::new(Mutex::new(table))))),
        Err(err) => Err(err),
    }
}

pub fn db_conn(conn_resource: ResourceArc<DbConnResource>) -> Connection {
    let connection;
    {
        connection = conn_resource
            .0
            .lock()
            .expect("Fatal: failed acquiring connection lock")
            .clone();
    }
    connection
}
