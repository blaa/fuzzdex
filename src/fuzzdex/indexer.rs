use std::collections::{HashMap, HashSet};

use crate::utils;
use super::*;
use super::seeker::*;

impl PhraseEntry {
    fn new(idx: usize, phrase: &str, constraints: Option<&HashSet<usize, FastHash>>) -> PhraseEntry {
        let constraints = constraints.map_or_else(
            || HashSet::with_hasher(FastHash::new()),
            |c| c.clone()
        );
        let phrase_tokens = utils::tokenize(phrase, 1);

        PhraseEntry {
            idx,
            origin: phrase.to_string(),
            tokens: phrase_tokens,
            constraints,
        }
    }
}

impl Indexer {
    /// Create a new empty fuzzdex in an "indexing" state.
    pub fn new() -> Indexer {
        Indexer {
            db: HashMap::with_capacity_and_hasher(32768, FastHash::new()),
            phrases: HashMap::with_hasher(FastHash::new()),
        }
    }

    fn add_token(&mut self, token: &str, phrase_idx: usize, token_idx: u32) {
        for trigram in utils::trigramize(token) {
            let entry = self.db.entry(trigram).or_insert(
                TrigramEntry { positions: Vec::new(), score: 0.0 }
            );
            entry.positions.push(Position { phrase_idx, token_idx });
            entry.score += 1.0;
        }
    }

    /// Add a phrase mapped to an index. Phrase can be found by one of it's
    /// fuzzy-matched tokens. Phrase index must be unique within the index (or
    /// Err is returned) and can reference some external dictionary.
    pub fn add_phrase(&mut self, phrase: &str, phrase_idx: usize,
                      constraints: Option<&HashSet<usize, FastHash>>) -> Result<(), DuplicateId> {
        if self.phrases.contains_key(&phrase_idx) {
            Err(DuplicateId {})
        } else {
            let entry = PhraseEntry::new(phrase_idx, phrase, constraints);
            for (token_idx, token) in entry.tokens.iter().enumerate() {
                self.add_token(token, phrase_idx, token_idx as u32);
            }
            self.phrases.insert(phrase_idx, entry);
            Ok(())
        }
    }

    /// Consume original Indexer and return Index class with querying ability
    /// and given internal cache size.
    pub fn finish_with_cache(mut self, cache_size: usize) -> Index {
        if self.db.is_empty() {
            return Index::new(self, cache_size);
        }

        /*
         * Having good scoring for trigrams allows to return good results when
         * the limit is set.
         *
         * For my two datasets data is rather skewed:
         * - 1-2782 range, 57 average, 8 median.
         * - 1-7280 range, 61 average, 7 median.
         * So there's always some very popular trigrams and way many more
         * "selective" ones.
         *
         * Let's try to put average count as "1".
         *
         * Hyperbolic function can smooth the scores and put them in nice range:
         * 0.5 + tanh(x)/2
         * Has range 0 - 1 for values -inf to inf (-3 to 3 de facto).
         * 0.5 + tanh((avg - val - 1) / avg)/2
         * Will have 0.5 at exactly average, distinguish all lower values
         * (higher score) up to 0.87, and will distinguish plenty of higher
         * values.
         */

        let average: f32 = self.db.values()
            .map(|v| v.positions.len())
            .sum::<usize>() as f32 / self.db.len() as f32;

        for (_trigram, entry) in self.db.iter_mut() {
            let input = entry.score;
            let score = 0.5 + ((average - input - 1.0) / average).tanh() / 2.0;
            entry.score = score;
        }
        Index::new(self, cache_size)
    }

    /// Consume original Indexer and return Index class with querying ability and default cache
    /// size of 500 entries (in our testcases 1000 entries is enough to have < 1% misses).
    pub fn finish(self) -> Index {
        self.finish_with_cache(2000)
    }
}

impl Default for Indexer {
    fn default() -> Self {
        Self::new()
    }
}


