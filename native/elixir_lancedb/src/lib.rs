mod schema;
mod atoms;
mod db;
use db::DbConnResource;
mod runtime;
use rustler::{Env, Term};


#[allow(unused, non_local_definitions)]
fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbConnResource, env);
    true
}

rustler::init!("Elixir.ElixirLanceDB.Native", load = load);
