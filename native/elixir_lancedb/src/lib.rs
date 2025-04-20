mod atoms;
mod db;
mod error;
mod runtime;
mod rustler_arrow;
mod table;

use db::DbConnResource;
use rustler::{Env, Term};
use table::TableResource;

#[allow(unused, non_local_definitions)]
fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbConnResource, env);
    rustler::resource!(TableResource, env);
    true
}

rustler::init!("Elixir.ElixirLanceDB.Native", load = load);
