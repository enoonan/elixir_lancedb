use lancedb::Connection;
use lancedb::{connect as lance_connect, Result, Table as LanceDbTable};
use tokio::runtime::Runtime;
use std::result::Result::{Ok, Err};
// use rustler::resource::ResourceArc;

// pub struct Connection {
//     inner: Option<LanceDBConnection>,
// }

#[rustler::nif]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

// #[rustler::nif]
// async fn connect(uri: &str) -> ResourceArc<Result<Connection>> {
//     let rt = Runtime::new().unwrap();

//     return { rt.block_on(async {
//         return ResourceArc.new(lance_connect(&uri).execute().await);
//     })};
// }

rustler::init!("Elixir.ElixirLanceDB.Native");