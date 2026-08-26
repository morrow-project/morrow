//! Bounded LRU cache for lazily activated partition resources.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub struct PartitionResourceCache<K, V> {
    capacity: usize,
    values: HashMap<K, V>,
    lru: VecDeque<K>,
    evictions: u64,
}

impl<K: Clone + Eq + Hash, V> PartitionResourceCache<K, V> {
    pub fn new(capacity: usize) -> Option<Self> {
        (capacity > 0).then_some(Self {
            capacity,
            values: HashMap::new(),
            lru: VecDeque::new(),
            evictions: 0,
        })
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if self.values.contains_key(key) {
            self.touch(key);
        }
        self.values.get(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self.values.insert(key.clone(), value);
        self.touch(&key);
        while self.values.len() > self.capacity {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if oldest != key {
                self.values.remove(&oldest);
                self.evictions = self.evictions.saturating_add(1);
            } else {
                self.lru.push_back(oldest);
            }
        }
        previous
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.lru.retain(|entry| entry != key);
        self.values.remove(key)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.lru.clear();
    }

    fn touch(&mut self, key: &K) {
        self.lru.retain(|entry| entry != key);
        self.lru.push_back(key.clone());
    }
}
