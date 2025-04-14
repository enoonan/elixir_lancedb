use lancedb::{connection::ConnectRequest, database::listing::ListingDatabase, Result};
use rustler::{Encoder, Env, ResourceArc, Term};
use std::{
    collections::HashMap,
    result::Result::{Err, Ok},
    sync::{Arc, Mutex},
};
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
struct DBResource(Arc<Mutex<ListingDatabase>>);

impl<'a> Encoder for DBResource {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let resource: ResourceArc<DBResource> = ResourceArc::new(DBResource(self.0.clone()));
        let result = resource.encode(env);
        print!("{:#?}", result);
        return result;
    }
}

// impl Decoder<'_> for DBResource<'_> {
//     fn decode(term: DBResource<'_>) -> NifResult<Self> {
//         term.try_into()
//     }
// }

/// Wrapper for BEAM reference terms.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Reference<'a>(Term<'a>);

#[rustler::nif]
fn connect(uri: String) -> ResourceArc<DBResource> {
    return get_connection(uri).unwrap().into();
}

fn get_connection(uri: String) -> Result<DBResource> {
    let runtime = Runtime::new().unwrap();
    return runtime.block_on(async {
        let request = ConnectRequest {
            uri: uri.to_string(),
            read_consistency_interval: None,
            options: HashMap::new(),
        };
        let db = ListingDatabase::connect_with_options(&request).await?;
        let resource = DBResource(Arc::new(Mutex::new(db)));
        return Ok(resource);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_conn() {
        let r = get_connection("./data".to_string());
        println!("{:#?}", r);
    }
}

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DBResource, env);
    true
}

rustler::init!("Elixir.ElixirLanceDB.Native", load = load);
