use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use crate::storage::Storage;

/// RAII Guard that executes a cleanup callback when dropped
pub struct TxCleanup<'a> {
    drop_fn: Option<Box<dyn FnOnce() + 'a>>,
}

impl<'a> TxCleanup<'a> {
    pub fn new<F: FnOnce() + 'a>(f: F) -> Self {
        Self {
            drop_fn: Some(Box::new(f)),
        }
    }
}

impl<'a> Drop for TxCleanup<'a> {
    fn drop(&mut self) {
        if let Some(f) = self.drop_fn.take() {
            f();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalChange<V> {
    Put(V),
    Deleted,
}

pub struct Transaction<'a, K, V> {
    pub(crate) start_tn: u64,
    pub(crate) read_set: HashSet<K>,
    pub(crate) write_set: HashMap<K, LocalChange<V>>,
    storage: &'a Storage<K, V>,
    _cleanup: Option<TxCleanup<'a>>,
}

impl<'a, K, V> Transaction<'a, K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub(crate) fn new_with_cleanup(
        start_tn: u64,
        storage: &'a Storage<K, V>,
        cleanup: TxCleanup<'a>,
    ) -> Self {
        Self {
            start_tn,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            storage,
            _cleanup: Some(cleanup),
        }
    }

    pub fn write(&mut self, key: K, value: V) {
        self.write_set.insert(key, LocalChange::Put(value));
    }

    pub fn create(&mut self, key: K, value: V) {
        self.write(key, value);
    }

    pub fn delete(&mut self, key: K) {
        self.write_set.insert(key, LocalChange::Deleted);
    }

    pub fn read(&mut self, key: &K) -> Option<V> {
        self.read_set.insert(key.clone());

        // Read-your-own-writes isolation
        if let Some(change) = self.write_set.get(key) {
            return match change {
                LocalChange::Put(val) => Some(val.clone()),
                LocalChange::Deleted => None,
            };
        }

        self.storage.get(key)
    }
}
