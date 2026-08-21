use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Default, Serialize, Deserialize)]
struct Checkpoints {
    generation: u64,
    offsets: BTreeMap<String, u64>,
}

pub struct CheckpointStore {
    path: PathBuf,
    state: Checkpoints,
}

impl CheckpointStore {
    pub fn open(path: impl Into<PathBuf>, generation: u64) -> Result<Self, String> {
        let path = path.into();
        let state = if path.exists() {
            serde_json::from_slice(&std::fs::read(&path).map_err(display)?).map_err(display)?
        } else {
            Checkpoints {
                generation,
                offsets: BTreeMap::new(),
            }
        };
        if state.generation > generation {
            return Err("checkpoint belongs to a newer connector generation".to_string());
        }
        Ok(Self { path, state })
    }

    pub fn offset(&self, stream: &str, partition: u32) -> Option<u64> {
        self.state.offsets.get(&key(stream, partition)).copied()
    }

    pub fn commit(
        &mut self,
        generation: u64,
        offsets: &BTreeMap<(String, u32), u64>,
    ) -> Result<(), String> {
        if generation < self.state.generation {
            return Err("stale connector generation".to_string());
        }
        self.state.generation = generation;
        for ((stream, partition), offset) in offsets {
            self.state
                .offsets
                .entry(key(stream, *partition))
                .and_modify(|current| *current = (*current).max(*offset))
                .or_insert(*offset);
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(display)?;
        }
        let temporary = self.path.with_extension("tmp");
        std::fs::write(
            &temporary,
            serde_json::to_vec(&self.state).map_err(display)?,
        )
        .map_err(display)?;
        std::fs::rename(temporary, &self.path).map_err(display)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn key(stream: &str, partition: u32) -> String {
    format!("{stream}:{partition}")
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
