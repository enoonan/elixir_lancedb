mod fts;
mod ivf_pq;

use fts::FtsIndexBuilderConfig;
use ivf_pq::IvfPqIndexBuilderConfig;
use lancedb::{
    index::{
        scalar::{BTreeIndexBuilder, BitmapIndexBuilder, LabelListIndexBuilder},
        Index, IndexConfig as LanceIndexConfig, IndexType,
    },
    DistanceType as LanceDistanceType,
};
use rustler::{Decoder, Encoder, NifResult, ResourceArc, Term};

use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
};

use super::{table_conn, TableResource};

#[derive(Debug)]
pub enum IndexConfig {
    Auto,
    BTree,
    Bitmap,
    LabelList,
    FullTextSearch(FtsIndexBuilderConfig),
    // IvfFlat(IvfFlatIndexBuilderConfig),
    IvfPq(IvfPqIndexBuilderConfig),
    // IvfHnswPq(IvfHnswPqIndexBuilderConfig),
    // IvfHnswSq(IvfHnswSqIndexBuilderConfig),
}

impl Into<Index> for IndexConfig {
    fn into(self) -> Index {
        match self {
            IndexConfig::Auto => Index::Auto,
            IndexConfig::BTree => Index::BTree(BTreeIndexBuilder {}),
            IndexConfig::Bitmap => Index::Bitmap(BitmapIndexBuilder {}),
            IndexConfig::LabelList => Index::LabelList(LabelListIndexBuilder {}),
            IndexConfig::IvfPq(cfg) => Index::IvfPq(cfg.into()),
            IndexConfig::FullTextSearch(cfg) => Index::FTS(cfg.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DistanceType {
    L2,
    Cosine,
    Dot,
    Hamming,
}

impl Into<LanceDistanceType> for DistanceType {
    fn into(self) -> LanceDistanceType {
        match self {
            DistanceType::L2 => LanceDistanceType::L2,
            DistanceType::Cosine => LanceDistanceType::Cosine,
            DistanceType::Dot => LanceDistanceType::Dot,
            DistanceType::Hamming => LanceDistanceType::Hamming,
        }
    }
}

impl Decoder<'_> for DistanceType {
    fn decode(term: Term<'_>) -> NifResult<Self> {
        let distance_type = match term.atom_to_string()?.as_str() {
            "l2" => Ok(DistanceType::L2),
            "cosine" => Ok(DistanceType::Cosine),
            "dot" => Ok(DistanceType::Dot),
            "hamming" => Ok(DistanceType::Hamming),
            _ => Err(rustler::Error::BadArg),
        }?;

        Ok(distance_type)
    }
}

// pub struct IvfFlatIndexBuilderConfig {
//     pub distance_type: DistanceType,
//     pub num_partitions: Option<u32>,
//     pub sample_rate: u32,
//     pub max_iterations: u32,
// }

// pub struct IvfHnswPqIndexBuilderConfig {
//     // IVF
//     pub distance_type: DistanceType,
//     pub num_partitions: Option<u32>,
//     pub sample_rate: u32,
//     pub max_iterations: u32,

//     // HNSW
//     pub m: u32,
//     pub ef_construction: u32,

//     // PQ
//     pub num_sub_vectors: Option<u32>,
//     pub num_bits: Option<u32>,
// }

// pub struct IvfHnswSqIndexBuilderConfig {
//     // IVF
//     pub distance_type: DistanceType,
//     pub num_partitions: Option<u32>,
//     pub sample_rate: u32,
//     pub max_iterations: u32,

//     // HNSW
//     pub m: u32,
//     pub ef_construction: u32,
// }
impl Decoder<'_> for IndexConfig {
    fn decode(term: Term) -> NifResult<Self> {
        let index_type = term.map_get(atoms::index_type())?;
        let result = match index_type.atom_to_string()?.as_str() {
            "auto" => IndexConfig::Auto,
            "btree" => IndexConfig::BTree,
            "bitmap" => IndexConfig::Bitmap,
            "label_list" => IndexConfig::LabelList,
            "ivf_pq" => IndexConfig::IvfPq(term.decode::<IvfPqIndexBuilderConfig>()?.into()),
            "fts" => IndexConfig::FullTextSearch(term.decode::<FtsIndexBuilderConfig>()?.into()),
            _ => todo!("not implemented"),
        };

        Ok(result)
    }
}

pub struct ReturnableIndexConfig(pub LanceIndexConfig);

impl Encoder for ReturnableIndexConfig {
    fn encode<'a>(&self, env: rustler::Env<'a>) -> Term<'a> {
        let index_type = match self.0.index_type {
            IndexType::IvfFlat => atoms::ivf_flat(),
            IndexType::BTree => atoms::btree(),
            IndexType::Bitmap => atoms::bitmap(),
            IndexType::FTS => atoms::fts(),
            IndexType::IvfHnswPq => atoms::ivf_hnsw_pq(),
            IndexType::IvfHnswSq => atoms::ivf_hnsw_sq(),
            IndexType::IvfPq => atoms::ivf_pq(),
            IndexType::LabelList => atoms::label_list(),
        };

        let mut map = Term::map_new(env);
        map = map.map_put(atoms::index_type(), index_type).unwrap_or(map);
        map = map
            .map_put(atoms::name(), self.0.name.encode(env))
            .unwrap_or(map);
        map = map
            .map_put(atoms::columns(), self.0.columns.encode(env))
            .unwrap_or(map);
        map
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn list_indices(table: ResourceArc<TableResource>) -> Result<Vec<ReturnableIndexConfig>> {
    let table = table_conn(table)?;
    let indices = get_runtime().block_on(async {
        let idcs = table
            .list_indices()
            .await?
            .iter()
            .map(|idx_cfg: &LanceIndexConfig| ReturnableIndexConfig(idx_cfg.clone()))
            .collect();
        Ok::<Vec<ReturnableIndexConfig>, Error>(idcs)
    })?;
    Ok(indices)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn create_index(
    table: ResourceArc<TableResource>,
    fields: Vec<String>,
    index_cfg: IndexConfig,
) -> Result<()> {
    let table = table_conn(table)?;
    get_runtime().block_on(async {
        let idx_builder = table.create_index(&fields, index_cfg.into());
        idx_builder.execute().await?;
        Ok::<(), Error>(())
    })?;

    Ok(())
}
