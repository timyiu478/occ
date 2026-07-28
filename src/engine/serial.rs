use super::OccEngine;
use crate::error::OccError;
use crate::storage::Storage;
use crate::transaction::{LocalChange, Transaction, TxCleanup};
use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

struct CommittedTx<K> {
    tx_id: u64,
    keys_modified: HashSet<K>,
}

pub struct SerialEngine<K, V> {
    storage: Storage<K, V>,
    global_tn: AtomicU64,
    // The lock protects the write phase and state
    state: Mutex<SerialEngineState<K>>,
}

struct SerialEngineState<K> {
    history: Vec<CommittedTx<K>>,
    /// Tracks active transactions by start_tn -> reference count
    active_snapshots: BTreeMap<u64, usize>,
}

impl<K, V> Default for SerialEngine<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> SerialEngine<K, V> {
    pub fn new() -> Self {
        Self {
            storage: Storage::new(),
            global_tn: AtomicU64::new(1),
            state: Mutex::new(SerialEngineState {
                history: Vec::new(),
                active_snapshots: BTreeMap::new(),
            }),
        }
    }

    /// Unregisters an active transaction snapshot and immediately prunes
    /// stale history entries up to the new active snapshot horizon.
    fn unregister_snapshot(&self, start_tn: u64) {
        let mut state = self.state.lock().unwrap();

        // 1. Deregister the start_tn from active snapshots
        if let std::collections::btree_map::Entry::Occupied(mut entry) =
            state.active_snapshots.entry(start_tn)
        {
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
        state
            .history
            .retain(|committed| committed.tx_id > min_active_tn);
    }
}

impl<'a, K, V> OccEngine<'a, K, V> for SerialEngine<K, V>
where
    K: Eq + Hash + Clone + 'a,
    V: Clone + 'a,
{
    fn begin(&'a self) -> Transaction<'a, K, V> {
        let start_tn = self.global_tn.load(Ordering::SeqCst);

        let mut state = self.state.lock().unwrap();
        *state.active_snapshots.entry(start_tn).or_default() += 1;
        drop(state); // Drop lock before creating Transaction

        // Attach RAII cleanup directly to the Transaction!
        let cleanup = TxCleanup::new(move || {
            self.unregister_snapshot(start_tn);
        });

        Transaction::new_with_cleanup(start_tn, &self.storage, cleanup)
    }

    fn commit(&self, tx: &mut Transaction<'a, K, V>) -> Result<(), OccError> {
        let mut state = self.state.lock().unwrap();

        // 1. Validation Phase
        for committed in state.history.iter().rev() {
            if committed.tx_id <= tx.start_tn {
                break;
            }
            if committed
                .keys_modified
                .iter()
                .any(|k| tx.read_set.contains(k))
            {
                return Err(OccError::ValidationFailed);
            }
        }

        if tx.write_set.is_empty() {
            return Ok(()); // Read-only fast path
        }

        // 2. Assign Commit Timestamp
        let commit_tn = self.global_tn.fetch_add(1, Ordering::SeqCst) + 1;

        // 3. Extract writes
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        let write_set = std::mem::take(&mut tx.write_set);

        for (key, change) in write_set {
            match change {
                LocalChange::Put(val) => puts.push((key, val)),
                LocalChange::Deleted => deletes.push(key),
            }
        }

        let mut modified_keys: HashSet<K> = puts.iter().map(|(k, _)| k.clone()).collect();
        modified_keys.extend(deletes.iter().cloned());

        // 4. Apply to storage and record commit
        self.storage.apply_batch(puts, deletes);
        state.history.push(CommittedTx {
            tx_id: commit_tn,
            keys_modified: modified_keys,
        });

        Ok(())
    }
}

#[cfg(test)]
mod internal_tests {
    use super::*;

    #[test]
    fn test_history_pruning_lifecycle() {
        let engine = SerialEngine::<&str, i32>::new();

        // 1. Long-running reader starts and holds start_tn
        let long_reader = engine.begin();
        assert_eq!(engine.state.lock().unwrap().history.len(), 0);

        // 2. Commit 5 separate transactions while long_reader is alive
        for i in 0..5 {
            engine
                .transaction(|tx| {
                    tx.write("key", i);
                    Ok(())
                })
                .unwrap();
        }

        // History MUST retain all 5 entries because long_reader pins the horizon
        assert_eq!(engine.state.lock().unwrap().history.len(), 5);

        // 3. Drop/finish the long-running reader
        drop(long_reader); // Calls SnapshotGuard::drop -> unregister_snapshot

        // History should now be pruned down to 0!
        assert_eq!(engine.state.lock().unwrap().history.len(), 0);
    }
}
