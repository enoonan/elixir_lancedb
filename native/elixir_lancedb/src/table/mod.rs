mod add;
mod count_rows;
mod delete;
mod index;
mod merge_insert;
mod optimize;
mod query;
mod update;

use std::sync::{Arc, Mutex};

use lancedb::Table;
use rustler::ResourceArc;

pub struct TableResource(pub Arc<Mutex<Table>>);

fn table_conn(table: ResourceArc<TableResource>) -> Table {
    let result;
    {
        result = table
            .0
            .lock()
            .expect("Fatal: failed acquiring table resource lock")
            .clone();
    }
    result
}
