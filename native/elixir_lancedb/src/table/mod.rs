mod add;
mod query;
use std::sync::{Arc, Mutex};

use lancedb::Table;
use rustler::ResourceArc;
pub struct TableResource(pub Arc<Mutex<Table>>);

fn table_conn(table_resource: ResourceArc<TableResource>) -> Table {
    let table;
    {
        table = table_resource
            .0
            .lock()
            .expect("Fatal: failed acquiring table resource lock")
            .clone();
    }
    table
}
