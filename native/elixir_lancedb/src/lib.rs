mod atoms;
mod conversion;
mod db;
mod runtime;
mod schema;

use db::DbConnResource;
use rustler::{Env, Term};

#[allow(unused, non_local_definitions)]
fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbConnResource, env);
    true
}

rustler::init!("Elixir.ElixirLanceDB.Native", load = load);
