pub fn validate_subject(subject: &str) -> bool {
    if subject.is_empty() || subject.starts_with('.') || subject.ends_with('.') {
        return false;
    }

    subject.split('.').all(|token| {
        !token.is_empty()
            && token != "*"
            && token != ">"
            && !token.contains('*')
            && !token.contains('>')
            && !token.chars().any(char::is_whitespace)
    })
}

pub fn validate_subscription(pattern: &str) -> bool {
    if pattern.is_empty() || pattern.starts_with('.') || pattern.ends_with('.') {
        return false;
    }

    let mut saw_tail = false;
    for (idx, token) in pattern.split('.').enumerate() {
        if token.is_empty() || saw_tail || token.chars().any(char::is_whitespace) {
            return false;
        }
        if token == ">" {
            saw_tail = true;
            if idx == 0 && pattern != ">" {
                return false;
            }
            continue;
        }
        if token != "*" && (token.contains('*') || token.contains('>')) {
            return false;
        }
    }
    true
}

pub fn matches(pattern: &str, subject: &str) -> bool {
    let mut pattern_tokens = pattern.split('.');
    let mut subject_tokens = subject.split('.');

    loop {
        match (pattern_tokens.next(), subject_tokens.next()) {
            (Some(">"), _) => return true,
            (Some("*"), Some(_)) => {}
            (Some(pattern), Some(subject)) if pattern == subject => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubjectTrie<T: Ord> {
    root: TrieNode<T>,
}

#[derive(Debug, Clone)]
struct TrieNode<T: Ord> {
    literals: HashMap<String, TrieNode<T>>,
    wildcard: Option<Box<TrieNode<T>>>,
    exact: BTreeSet<T>,
    tail: BTreeSet<T>,
}

impl<T: Ord> Default for SubjectTrie<T> {
    fn default() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }
}

impl<T: Ord> Default for TrieNode<T> {
    fn default() -> Self {
        Self {
            literals: HashMap::new(),
            wildcard: None,
            exact: BTreeSet::new(),
            tail: BTreeSet::new(),
        }
    }
}

impl<T: Clone + Ord> SubjectTrie<T> {
    pub fn insert(&mut self, pattern: &str, value: T) -> bool {
        if !validate_subscription(pattern) {
            return false;
        }
        let mut node = &mut self.root;
        for token in pattern.split('.') {
            match token {
                ">" => return node.tail.insert(value),
                "*" => node = node.wildcard.get_or_insert_with(Default::default),
                literal => node = node.literals.entry(literal.to_string()).or_default(),
            }
        }
        node.exact.insert(value)
    }

    pub fn remove(&mut self, pattern: &str, value: &T) -> bool {
        if !validate_subscription(pattern) {
            return false;
        }
        let mut node = &mut self.root;
        for token in pattern.split('.') {
            match token {
                ">" => return node.tail.remove(value),
                "*" => {
                    let Some(wildcard) = node.wildcard.as_mut() else {
                        return false;
                    };
                    node = wildcard;
                }
                literal => {
                    let Some(literal) = node.literals.get_mut(literal) else {
                        return false;
                    };
                    node = literal;
                }
            }
        }
        node.exact.remove(value)
    }

    pub fn matching(&self, subject: &str) -> Vec<T> {
        if !validate_subject(subject) {
            return Vec::new();
        }
        let tokens = subject.split('.').collect::<Vec<_>>();
        let mut matches = BTreeSet::new();
        collect_matches(&self.root, &tokens, 0, &mut matches);
        matches.into_iter().collect()
    }

    pub fn matches_any(&self, subject: &str) -> bool {
        if !validate_subject(subject) {
            return false;
        }
        matches_node(&self.root, &subject.split('.').collect::<Vec<_>>(), 0)
    }
}

fn collect_matches<T: Clone + Ord>(
    node: &TrieNode<T>,
    tokens: &[&str],
    index: usize,
    matches: &mut BTreeSet<T>,
) {
    matches.extend(node.tail.iter().cloned());
    if index == tokens.len() {
        matches.extend(node.exact.iter().cloned());
        return;
    }
    if let Some(literal) = node.literals.get(tokens[index]) {
        collect_matches(literal, tokens, index + 1, matches);
    }
    if let Some(wildcard) = &node.wildcard {
        collect_matches(wildcard, tokens, index + 1, matches);
    }
}

fn matches_node<T: Ord>(node: &TrieNode<T>, tokens: &[&str], index: usize) -> bool {
    if !node.tail.is_empty() {
        return true;
    }
    if index == tokens.len() {
        return !node.exact.is_empty();
    }
    node.literals
        .get(tokens[index])
        .is_some_and(|literal| matches_node(literal, tokens, index + 1))
        || node
            .wildcard
            .as_deref()
            .is_some_and(|wildcard| matches_node(wildcard, tokens, index + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_publish_subjects() {
        assert!(validate_subject("orders.created"));
        assert!(!validate_subject("orders.*"));
        assert!(!validate_subject(".orders"));
        assert!(!validate_subject("orders."));
        assert!(!validate_subject("orders..created"));
    }

    #[test]
    fn validates_subscription_patterns() {
        assert!(validate_subscription("orders.*"));
        assert!(validate_subscription("orders.>"));
        assert!(validate_subscription(">"));
        assert!(!validate_subscription("orders.>.created"));
        assert!(!validate_subscription("orders.foo*"));
    }

    #[test]
    fn matches_nats_wildcards() {
        assert!(matches("orders.*", "orders.created"));
        assert!(!matches("orders.*", "orders.us.created"));
        assert!(matches("orders.>", "orders.us.created"));
        assert!(matches(">", "anything"));
        assert!(!matches("orders.created", "orders.deleted"));
    }

    #[test]
    fn trie_matches_reference_for_generated_patterns_and_subjects() {
        let literals = ["a", "b", "c"];
        let mut patterns = Vec::new();
        for first in literals {
            patterns.push(first.to_string());
            patterns.push(format!("{first}.*"));
            patterns.push(format!("{first}.>"));
            for second in literals {
                patterns.push(format!("{first}.{second}"));
                patterns.push(format!("{first}.{second}.*"));
            }
        }
        patterns.push(">".to_string());
        let mut trie = SubjectTrie::default();
        for (id, pattern) in patterns.iter().enumerate() {
            assert!(trie.insert(pattern, id));
        }
        for first in literals {
            for subject in [
                first.to_string(),
                format!("{first}.a"),
                format!("{first}.b.c"),
            ] {
                let expected = patterns
                    .iter()
                    .enumerate()
                    .filter(|(_, pattern)| matches(pattern, &subject))
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>();
                assert_eq!(trie.matching(&subject), expected, "subject {subject}");
            }
        }
    }

    #[test]
    fn trie_removal_preserves_other_interests() {
        let mut trie = SubjectTrie::default();
        trie.insert("orders.*", 1);
        trie.insert("orders.>", 2);
        assert!(trie.remove("orders.*", &1));
        assert_eq!(trie.matching("orders.created"), vec![2]);
        assert_eq!(trie.matching("orders.eu.created"), vec![2]);
    }

    #[test]
    #[ignore = "manual routing microbenchmark"]
    fn benchmark_trie_exact_star_and_tail_matching() {
        use std::time::Instant;

        let mut trie = SubjectTrie::default();
        let mut patterns = Vec::new();
        for id in 0..10_000usize {
            let pattern = match id % 3 {
                0 => format!("tenant.{id}.event"),
                1 => format!("tenant.*.event{id}"),
                _ => format!("tenant.{id}.>"),
            };
            trie.insert(&pattern, id);
            patterns.push(pattern);
        }
        let subjects = [
            "tenant.9999.event",
            "tenant.any.event1",
            "tenant.9998.deep.event",
        ];
        let started = Instant::now();
        for _ in 0..1_000 {
            for subject in subjects {
                std::hint::black_box(trie.matching(subject));
            }
        }
        let trie_elapsed = started.elapsed();
        let started = Instant::now();
        for _ in 0..1_000 {
            for subject in subjects {
                std::hint::black_box(
                    patterns
                        .iter()
                        .filter(|pattern| matches(pattern, subject))
                        .count(),
                );
            }
        }
        eprintln!("trie={trie_elapsed:?} scan={:?}", started.elapsed());
    }
}
use std::collections::{BTreeSet, HashMap};
