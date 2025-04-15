use lancedb::Connection;
use once_cell::sync::OnceCell;
use rustler::{Env, ResourceArc, Term};
use std::{
    result::Result::{Err, Ok},
    sync::{Arc, Mutex},
};
use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create ElixirLanceDB runtime")
    })
}

#[derive(Clone)]
struct DbConnResource(Arc<Mutex<Connection>>);

#[rustler::nif]
fn connect(uri: String) -> ResourceArc<DbConnResource> {
    let conn = get_runtime()
        .block_on(async { lancedb::connect(&uri).execute().await })
        .unwrap();
    ResourceArc::new(DbConnResource(Arc::new(Mutex::new(conn))))
}

#[rustler::nif]
fn table_names(conn: ResourceArc<DbConnResource>) -> Vec<String> {
    let conn = db_conn(conn);
    return get_runtime().block_on(async { conn.table_names().execute().await.unwrap() });
}

fn db_conn(conn_resource: ResourceArc<DbConnResource>) -> Connection {
    let connection;
    {
        connection = conn_resource.0.lock().unwrap().clone();
    }
    connection
}

#[allow(unused, non_local_definitions)]
fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbConnResource, env);
    true
}

rustler::init!("Elixir.ElixirLanceDB.Native", load = load);
