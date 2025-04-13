use lancedb::Connection;
use lancedb::{connect as lance_connect, Result, Table as LanceDbTable};
use tokio::runtime::Runtime;
use std::fmt::Error;
use std::result::Result::{Ok, Err};
use rustler;
use rustler::ResourceArc;
use std::sync::Mutex;

// pub struct Connection {
//     inner: Option<LanceDBConnection>,
// }

// #[rustler::resource_impl]
// impl rustler::Resource for Connection {}

#[rustler::nif]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

// #[rustler::nif]
// fn connect(uri: &str) -> Result<ResourceArc<Mutex<Connection>>, &'static str> {
//     let rt = Runtime::new().unwrap();
//     rt.block_on(async {
//         match lance_connect(&uri).execute().await {
//             Ok(conn) => Ok(ResourceArc::new(Mutex::new(conn))),
//             Err(_) => Err("Failed to connect"),
//         }
//     })
// }

rustler::init!("Elixir.ElixirLanceDB.Native");