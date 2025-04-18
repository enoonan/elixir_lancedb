mod atoms;
mod db;
mod error;
mod runtime;
mod schema;
mod term_from_arrow;
mod term_to_arrow;

use db::DbConnResource;
use rustler::{Env, Term};

#[allow(unused, non_local_definitions)]
fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbConnResource, env);
    true
}

rustler::init!("Elixir.ElixirLanceDB.Native", load = load);
