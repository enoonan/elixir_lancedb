use crate::{
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::{schema, term_to_arrow},
    table::TableResource,
};
use arrow_array::{RecordBatch, RecordBatchIterator};
use lancedb::{Connection, Table};
use rustler::{resource_impl, Resource, ResourceArc, Term};

use std::sync::{Arc, Mutex};
pub struct DbConnResource(pub Arc<Mutex<Connection>>);

#[resource_impl]
impl Resource for DbConnResource{}

#[rustler::nif(schedule = "DirtyCpu")]
fn connect(uri: String) -> Result<ResourceArc<DbConnResource>> {
    let result = get_runtime().block_on(async {
        let conn = lancedb::connect(&uri).execute().await?;
        Ok::<Connection, Error>(conn)
    })?;

    let conn_resource = DbConnResource(Arc::new(Mutex::new(result)));

    Ok(ResourceArc::new(conn_resource))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_names(conn: ResourceArc<DbConnResource>) -> Result<Vec<String>> {
    let conn = db_conn(conn);

    let result = get_runtime().block_on(async {
        let names = conn.table_names().execute().await?;
        Ok::<Vec<String>, Error>(names)
    })?;

    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_all_tables(conn: ResourceArc<DbConnResource>) -> Result<()> {
    let conn = db_conn(conn);

    get_runtime().block_on(async {
        conn.drop_all_tables().await?;
        Ok::<(), Error>(())
    })?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_table(conn: ResourceArc<DbConnResource>, table_name: String) -> Result<()> {
    let conn = db_conn(conn);

    get_runtime().block_on(async {
        conn.drop_table(&table_name).await?;
        Ok::<(), Error>(())
    })?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn create_empty_table(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    schema: schema::Schema,
) -> Result<ResourceArc<TableResource>> {
    let conn = db_conn(conn);

    let table = get_runtime().block_on(async {
        let table = conn
            .create_empty_table(table_name, Arc::new(schema.into()))
            .execute()
            .await?;

        Ok::<Table, Error>(table)
    })?;

    let table_arc = TableResource(Arc::new(Mutex::new(table)));

    Ok(ResourceArc::new(table_arc))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn create_table_with_data(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    erl_data: Term,
    erl_schema: schema::Schema,
) -> Result<ResourceArc<TableResource>> {
    let arrow_schema: arrow_schema::Schema = erl_schema.into();
    let arc_schema = Arc::new(arrow_schema.clone());
    let columnar_data = term_to_arrow::to_arrow(erl_data, arrow_schema.clone())?;
    let batch = RecordBatchIterator::new(
        vec![RecordBatch::try_new(arc_schema.clone(), columnar_data)],
        arc_schema,
    );

    let conn = db_conn(conn);
    let table = get_runtime().block_on(async {
        let table = conn
            .create_table(table_name, Box::new(batch))
            .execute()
            .await?;
        Ok::<Table, Error>(table)
    })?;

    let table_arc = TableResource(Arc::new(Mutex::new(table)));

    Ok(ResourceArc::new(table_arc))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn open_table(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
) -> Result<ResourceArc<TableResource>> {
    let conn = db_conn(conn);
    let table = get_runtime().block_on(async {
        let table = conn.open_table(table_name).execute().await?;
        Ok::<Table, Error>(table)
    })?;

    let table_arc = TableResource(Arc::new(Mutex::new(table)));

    Ok(ResourceArc::new(table_arc))
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
