use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
};

use super::{table_conn, TableResource};
use lance::dataset::{
    cleanup::RemovalStats as LanceRemovalStats,
    optimize::CompactionMetrics as LanceCompactionMetrics,
};

// use lance::dataset::{
//     cleanup::RemovalStats as LanceRemovalStats,
//     optimize::CompactionMetrics as LanceCompactionMetrics,
// };
use lancedb::table::{OptimizeAction as LanceOptimizeAction, OptimizeStats as LanceOptimizeStats};
use rustler::{Decoder, NifMap, ResourceArc};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn optimize(
    table: ResourceArc<TableResource>,
    action: OptimizeAction,
) -> Result<OptimizeStats> {
    let table = table_conn(table)?;
    let result: OptimizeStats = get_runtime()
        .block_on(async {
            let result = table.optimize(action.into()).await?;
            Ok::<LanceOptimizeStats, Error>(result)
        })?
        .into();

    Ok(result)
}

#[derive(NifMap)]
pub struct OptimizeStats {
    pub compaction: Option<CompactionMetrics>,
    pub prune: Option<RemovalStats>,
}

impl From<LanceOptimizeStats> for OptimizeStats {
    fn from(value: LanceOptimizeStats) -> Self {
        OptimizeStats {
            compaction: match value.compaction {
                Some(compaction) => Some(CompactionMetrics {
                    fragments_removed: compaction.fragments_removed,
                    fragments_added: compaction.fragments_added,
                    files_removed: compaction.files_removed,
                    files_added: compaction.files_added,
                }),
                None => None,
            },
            prune: match value.prune {
                Some(prune) => Some(RemovalStats {
                    bytes_removed: prune.bytes_removed,
                    old_versions: prune.old_versions,
                }),
                None => None,
            },
        }
    }
}
#[derive(NifMap)]

pub struct CompactionMetrics {
    pub fragments_removed: usize,
    pub fragments_added: usize,
    pub files_removed: usize,
    pub files_added: usize,
}

impl From<LanceCompactionMetrics> for CompactionMetrics {
    fn from(value: LanceCompactionMetrics) -> Self {
        CompactionMetrics {
            fragments_removed: value.fragments_removed,
            fragments_added: value.fragments_added,
            files_removed: value.files_removed,
            files_added: value.files_added,
        }
    }
}

#[derive(NifMap)]
pub struct RemovalStats {
    pub bytes_removed: u64,
    pub old_versions: u64,
}

impl From<LanceRemovalStats> for RemovalStats {
    fn from(value: LanceRemovalStats) -> Self {
        RemovalStats {
            bytes_removed: value.bytes_removed,
            old_versions: value.old_versions,
        }
    }
}

pub enum OptimizeAction {
    All,
}

impl Decoder<'_> for OptimizeAction {
    fn decode(term: rustler::Term<'_>) -> rustler::NifResult<Self> {
        let action = term.map_get(atoms::action_type())?.atom_to_string()?;
        let result = match action.as_str() {
            "all" => OptimizeAction::All,
            _ => todo!("only optimize action All is implemented"),
        };

        Ok(result)
    }
}

impl Into<LanceOptimizeAction> for OptimizeAction {
    fn into(self) -> LanceOptimizeAction {
        match self {
            OptimizeAction::All => LanceOptimizeAction::All,
        }
    }
}
