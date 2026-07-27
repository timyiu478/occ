use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct Storage<K, V> {
    data: RwLock<HashMap<K, V>>,
}

impl<K, V> Storage<K, V>
{
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl<K, V> Storage<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Fast, shared-read lookup
    pub fn get(&self, key: &K) -> Option<V> {
        self.data.read().unwrap().get(key).cloned()
    }

    /// Atomic batch application (Puts and Deletes)
    pub fn apply_batch(&self, puts: Vec<(K, V)>, deletes: Vec<K>) {
        if puts.is_empty() && deletes.is_empty() {
            return;
        }

        let mut map = self.data.write().unwrap();
        for (key, val) in puts {
            map.insert(key, val);
        }
        for key in deletes {
            map.remove(&key);
        }
    }
}
