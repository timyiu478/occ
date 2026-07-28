use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

const NUM_SHARDS: usize = 64;

pub struct Storage<K, V> {
    shards: Vec<RwLock<HashMap<K, V>>>,
}

impl<K, V> Storage<K, V> {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }
}

impl<K, V> Storage<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Hashes the key to find which independent lock bucket it belongs to
    fn get_shard_index(key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }

    /// Readers only acquire a read-lock on the specific shard they need
    pub fn get(&self, key: &K) -> Option<V> {
        let shard_idx = Self::get_shard_index(key);
        let shard = self.shards[shard_idx].read().unwrap();
        shard.get(key).cloned()
    }

    /// Applies writes concurrently. Disjoint writes hitting different shards
    /// will not block each other.
    pub fn apply_batch(&self, puts: Vec<(K, V)>, deletes: Vec<K>) {
        // 1. Group all incoming writes by their target shard
        let mut sharded_puts: Vec<Vec<(K, V)>> = vec![Vec::new(); NUM_SHARDS];
        let mut sharded_deletes: Vec<Vec<K>> = vec![Vec::new(); NUM_SHARDS];

        for (k, v) in puts {
            let idx = Self::get_shard_index(&k);
            sharded_puts[idx].push((k, v));
        }

        for k in deletes {
            let idx = Self::get_shard_index(&k);
            sharded_deletes[idx].push(k);
        }

        // 2. Apply writes shard-by-shard.
        // NOTE: Iterating from 0 to NUM_SHARDS guarantees we always lock in a
        // strictly ascending order, which mathematically prevents ABBA deadlocks
        for i in 0..NUM_SHARDS {
            let has_puts = !sharded_puts[i].is_empty();
            let has_deletes = !sharded_deletes[i].is_empty();

            if has_puts || has_deletes {
                // We only grab a write lock for the specific shard being modified
                let mut shard_lock = self.shards[i].write().unwrap();

                if has_puts {
                    for (k, v) in std::mem::take(&mut sharded_puts[i]) {
                        shard_lock.insert(k, v);
                    }
                }
                if has_deletes {
                    for k in std::mem::take(&mut sharded_deletes[i]) {
                        shard_lock.remove(&k);
                    }
                }
            }
        }
    }
}
