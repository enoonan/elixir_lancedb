use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
    schema, term_from_arrow, term_to_arrow,
};
use arrow_array::{RecordBatch, RecordBatchIterator};
use futures::TryStreamExt;

use lancedb::query::ExecutableQuery;
use lancedb::Connection;
use rustler::{Atom, ResourceArc, Term};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
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
fn query_table<'a>(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
) -> Result<Vec<HashMap<String, term_from_arrow::Value>>> {
    let conn = db_conn(conn);

    let result: Result<Vec<HashMap<String, term_from_arrow::Value>>> =
        get_runtime().block_on(async {
            let tbl = conn.open_table(table_name).execute().await?;

            let schema = tbl.schema().await?;
            let results: Vec<RecordBatch> = tbl.query().execute().await?.try_collect().await?;

            term_from_arrow::term_from_arrow(results, schema.as_ref())
        });

    match result {
        Ok(recs) => Ok(recs),
        Err(err) => Err(Error::from(err)),
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
