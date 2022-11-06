use std::collections::HashMap;
use std::collections::HashSet;

use crate::utils;
use super::*;
use super::seeker::*;

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

    /// Add a phrase mapped to an index. Phrase can be found by one of it's fuzzy-matched tokens.
    pub fn add_phrase(&mut self, phrase: &str, phrase_idx: usize,
                      constraints: Option<&HashSet<usize, FastHash>>) {
        let phrase_tokens = utils::tokenize(phrase, 1);
        for (token_idx, token) in phrase_tokens.iter().enumerate() {
            self.add_token(token, phrase_idx, token_idx as u32);
        }
        let constraints = match constraints {
            Some(constraints) => constraints.clone(),
            None => HashSet::with_hasher(FastHash::new())
        };

        /* TODO: Migrate to PhraseEntry::new */
        self.phrases.insert(phrase_idx, PhraseEntry {
            idx: phrase_idx,
            origin: phrase.to_string(),
            tokens: phrase_tokens,
            constraints,
        });
    }

    /// Consume original Indexer and return Index class with querying ability.
    pub fn finish(mut self) -> Index {
        if self.db.is_empty() {
            return Index::new(self);
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
        Index::new(self)
    }
}

impl Default for Indexer {
    fn default() -> Self {
        Self::new()
    }
}


