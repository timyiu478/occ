use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use crate::error::OccError;
use crate::storage::Storage;
use crate::transaction::{LocalChange, Transaction, TxCleanup};
use super::OccEngine;


struct CommittedTx<K> {
    tx_id: u64,
    keys_modified: HashSet<K>,
}

pub struct ParallelEngine<K, V> {
    storage: Storage<K, V>,
    global_tn: AtomicU64,    // Used for committed transaction sequence numbers (tn)
    next_tx_id: AtomicU64,   // Used to assign unique active IDs
    state: RwLock<ParallelEngineState<K>>,
}

struct ParallelEngineState<K> {
    history: Vec<CommittedTx<K>>,
    /// Tracks active transactions by start_tn -> reference count
    active_snapshots: BTreeMap<u64, usize>,
    /// Tracks transactions currently in the 'tend' validation/write phase
    /// Maps TxId -> write_set of the validating transaction
    active_validating: BTreeMap<u64, HashSet<K>>,
}

impl<K, V> ParallelEngine<K, V> {
    pub fn new() -> Self {
        Self {
            storage: Storage::new(),
            global_tn: AtomicU64::new(1),
            next_tx_id: AtomicU64::new(1),
            state: RwLock::new(ParallelEngineState{
                history: Vec::new(),
                active_validating: BTreeMap::new(),
                active_snapshots: BTreeMap::new(),
            }),
        }
    }

    /// Unregisters an active transaction snapshot and immediately prunes
    /// stale history entries up to the new active snapshot horizon.
    fn unregister_snapshot(&self, start_tn: u64) {
        let mut state = self.state.write().unwrap();

        // 1. Deregister the start_tn from active snapshots
        if let std::collections::btree_map::Entry::Occupied(mut entry) = state.active_snapshots.entry(start_tn) {
            *entry.get_mut() -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }

        // 2. Compute the new minimum active snapshot point.
        // If no transactions are active, everything up to current global_tn is safe to prune.
        let min_active_tn = state
            .active_snapshots
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| self.global_tn.load(Ordering::SeqCst));

        // 3. Prune history entries that are strictly older than or equal to min_active_tn
        state.history.retain(|committed| committed.tx_id > min_active_tn);
    }

}

impl<'a, K, V> OccEngine<'a, K, V> for ParallelEngine<K, V>
where
    K: Eq + Hash + Clone + 'a,
    V: Clone + 'a,
{
    fn begin(&'a self) -> Transaction<'a, K, V> {
        let start_tn = self.global_tn.load(Ordering::SeqCst);

        let mut state = self.state.write().unwrap();
        *state.active_snapshots.entry(start_tn).or_default() += 1;
        drop(state);

        let cleanup = TxCleanup::new(move || {
            self.unregister_snapshot(start_tn);
        });

        Transaction::new_with_cleanup(start_tn, &self.storage, cleanup)
    }


    fn commit(&self, tx: &mut Transaction<'a, K, V>) -> Result<(), OccError> {
        let active_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let write_keys: HashSet<K> = tx.write_set.keys().cloned().collect();

        let finish_tn: u64;
        let finish_active: Vec<HashSet<K>>;

        {
            let mut state = self.state.write().unwrap();

            finish_tn = self.global_tn.load(Ordering::SeqCst);
            finish_active = state.active_validating.values().cloned().collect();
            
            state.active_validating.insert(active_id, write_keys.clone());
        }

        let mut valid = true;

        // Validate against COMMITTED transactions (start_tn + 1 to finish_tn)
        {
            let state = self.state.read().unwrap(); // SHARED read lock!
 
            for committed in state.history.iter().rev() {
                if committed.tx_id <= tx.start_tn {
                    break;
                }
                if committed.tx_id > finish_tn {
                    continue;
                }
                if committed.keys_modified.iter().any(|k| tx.read_set.contains(k)) {
                    valid = false;
                    break;
                }
            }
        }

        // Validate against ACTIVE validating transactions (finish_active)
        if valid {
            for active_write_set in finish_active {
                if active_write_set.iter().any(|k| tx.read_set.contains(k) || write_keys.contains(k)) {
                    valid = false;
                    break;
                }
            }
        }

        if valid && write_keys.is_empty() {
            let mut state = self.state.write().unwrap();
            state.active_validating.remove(&active_id);
            return Ok(()); 
        }

        if valid {
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            for (key, change) in std::mem::take(&mut tx.write_set) {
                match change {
                    LocalChange::Put(val) => puts.push((key, val)),
                    LocalChange::Deleted => deletes.push(key),
                }
            }
            self.storage.apply_batch(puts, deletes);

            let commit_tn = self.global_tn.fetch_add(1, Ordering::SeqCst) + 1;

            let mut state = self.state.write().unwrap();

            state.active_validating.remove(&active_id);
            state.history.push(CommittedTx {
                tx_id: commit_tn,
                keys_modified: write_keys,
            });

            Ok(())
        } else {
            let mut state = self.state.write().unwrap();
            state.active_validating.remove(&active_id);
            Err(OccError::ValidationFailed)
        }
    }
}
