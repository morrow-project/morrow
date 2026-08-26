//! Stable ownership keys for hot-path state sharding.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateShardKey<'a> {
    Partition { stream: &'a str, partition: u32 },
    Consumer(&'a str),
    Producer(&'a str),
    Tenant(&'a str),
}

/// Select a stable shard without allocating or using process-randomized
/// hashing. The same key therefore maps to the same owner after a restart.
pub fn shard_for(key: StateShardKey<'_>, shard_count: usize) -> usize {
    assert!(shard_count > 0, "state shard count must be non-zero");
    let mut hash = 0xcbf29ce484222325_u64;
    match key {
        StateShardKey::Partition { stream, partition } => {
            hash_bytes(&mut hash, stream.as_bytes());
            hash_bytes(&mut hash, &partition.to_be_bytes());
        }
        StateShardKey::Consumer(value) => {
            hash_bytes(&mut hash, b"consumer:");
            hash_bytes(&mut hash, value.as_bytes());
        }
        StateShardKey::Producer(value) => {
            hash_bytes(&mut hash, b"producer:");
            hash_bytes(&mut hash, value.as_bytes());
        }
        StateShardKey::Tenant(value) => {
            hash_bytes(&mut hash, b"tenant:");
            hash_bytes(&mut hash, value.as_bytes());
        }
    }
    (hash as usize) % shard_count
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x100000001b3);
    }
}
