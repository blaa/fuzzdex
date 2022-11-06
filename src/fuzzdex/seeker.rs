use std::collections::HashMap;
use std::cmp::Ordering;
use itertools::Itertools;

use std::sync::Arc;
use std::sync::Mutex;
use lru::LruCache;

use crate::utils;
use super::query::Query;
use super::{Indexer, FastHash};


/// Query result
#[derive(Debug, Clone, PartialEq)]
pub struct Result<'a> {
    /// Original matched phrase before tokenization.
    pub origin: &'a str,
    /// Returned index, a "value" of dictionary.
    pub index: usize,
    /// Token that matched the must token.
    pub token: &'a str,
    /// Token distance to the query.
    pub distance: usize,
    /// Trigram score of the phrase.
    pub score: f32,
    /// Bonus score from /should/ tokens.
    pub should_score: f32,
}

/* Trigram heatmap is a partial query result */
#[derive(Debug, Clone)]
struct PhraseHeatmap {
    /// Phrase Index
    phrase_idx: usize,
    /// Token trigram score: token_idx -> score
    tokens: HashMap<u32, f32, FastHash>,
    /// Total phrase score
    total_score: f32,
}

impl PhraseHeatmap {
    fn new(phrase_idx: usize) -> PhraseHeatmap {
        PhraseHeatmap {
            phrase_idx,
            tokens: HashMap::with_hasher(FastHash::new()),
            total_score: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
struct Heatmap {
    /* Trigram score */
    /* phrase_idx -> token_idx -> score */
    phrases: HashMap<usize, PhraseHeatmap, FastHash>,
    /* Max phrase score */
    max_score: f32,
}

impl Heatmap {
    fn new() -> Heatmap {
        Heatmap {
            phrases: HashMap::with_capacity_and_hasher(8, FastHash::new()),
            max_score: 0.0,
        }
    }
}

/// Produced by Index::finish() and can be queried.
pub struct Index {
    /// Index prepared for querying.
    pub index: Indexer,

    /// LRU cache of must tokens.
    cache: Mutex<LruCache<String, Arc<Heatmap>, FastHash>>,
}

impl Index {

    pub fn new(indexer: Indexer) -> Index {
        Index {
            index: indexer,
            cache: Mutex::new(LruCache::with_hasher(30000, FastHash::new())),
        }
    }

    /// Create a trigram heatmap for a given token.
    fn create_heatmap(&self, token: &str) -> Arc<Heatmap> {
        let index = &self.index;
        let db = &index.db;

        /* LRU cache updates position even on get and needs mutable reference */
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(heatmap) = cache.get(token) {
                /* We operate on reference-counted heatmaps to eliminate unnecessary copying */
                return heatmap.clone();
            }
        }

        let mut heatmap = Heatmap::new();

        for trigram in utils::trigramize(token) {
            if let Some(entry) = db.get(&trigram) {
                for position in entry.positions.iter() {
                    /* Get or create phrase-level entry */
                    let phrase_heatmap = heatmap.phrases.entry(position.phrase_idx).or_insert_with(
                        || PhraseHeatmap::new(position.phrase_idx));

                    /* Get or create token-level entry */
                    let token_score = phrase_heatmap.tokens.entry(position.token_idx).or_insert(0.0);
                    *token_score += entry.score;

                    phrase_heatmap.total_score += entry.score;
                    if phrase_heatmap.total_score > heatmap.max_score {
                        heatmap.max_score = phrase_heatmap.total_score;
                    }
                }
            }
        }

        let heatmap = Arc::new(heatmap);
        {
            let mut cache = self.cache.lock().unwrap();
            cache.put(token.to_string(), heatmap.clone());
        }
        heatmap
    }

    fn should_scores(&self, heatmap: &Heatmap, should_tokens: &[String])
                     -> HashMap<usize, f32, FastHash> {
        let mut map: HashMap<usize, f32, FastHash> = HashMap::with_capacity_and_hasher(
            heatmap.phrases.len(), FastHash::new()
        );
        let db = &self.index.db;

        for token in should_tokens {
            let mut trigrams = utils::trigramize(token);
            /* Use only first 4 trigrams for should scores */
            trigrams.truncate(3);
            for trigram in utils::trigramize(token) {
                if let Some(entry) = db.get(&trigram) {
                    for position in entry.positions.iter() {
                        if heatmap.phrases.contains_key(&position.phrase_idx) {
                            /* This phrase is within heatmap, we can calculate should score */
                            let score = map.entry(position.phrase_idx).or_insert(0.0);
                            *score += entry.score;
                        }
                    }
                }
            }
        }
        map
    }

    fn filtered_results(&self, query: &Query, heatmap: &Heatmap,
                        should_scores: HashMap<usize, f32, FastHash>) -> Vec<Result> {
        let mut results: Vec<Result> = Vec::with_capacity(query.limit.unwrap_or(3));
        if let Some(limit) = query.limit {
            results.reserve(limit);
        }
        let index = &self.index;
        let max_distance: usize = query.max_distance.unwrap_or(usize::MAX);
        let limit: usize = query.limit.unwrap_or(usize::MAX);

        /*
         * Sort phrases by a trigram score. This is an approximation as our
         * final metric - edit distance is better, but expensive to calculate.
         * Phrases with higher score have a higher probability of having lower edit distance,
         * but that is not certain.
         */
        let phrases_by_score = heatmap.phrases
            .values()
            .filter_map(|phrase_heatmap| {
                /* Add phrase data to iterator */
                let phrase = &index.phrases[&phrase_heatmap.phrase_idx];
                let should_score = *should_scores.get(&phrase_heatmap.phrase_idx).unwrap_or(&0.0);
                let extended = (phrase_heatmap,
                                phrase, should_score);
                if let Some(constraint) = query.constraint {
                    /* Check constraint from query */
                    if phrase.constraints.contains(&constraint) {
                        Some(extended)
                    } else {
                        None
                    }
                } else {
                    /* No constraint - return all */
                    Some(extended)
                }
            })
            .sorted_by(|(heat_a, phrase_a, should_a), (heat_b, phrase_b, should_b)| {
                /* Sort by score and then by a should score; for identical - prefer shortest. */
                let side_a = (heat_b.total_score, should_b, phrase_a.origin.len());
                let side_b = (heat_a.total_score, should_a, phrase_b.origin.len());
                side_a.partial_cmp(&side_b).expect("Some scores were NaN, and they shouldn't")
            });

        /* Best distance so far */
        let mut best_distance: usize = usize::MAX;

        for (phrase_heatmap, phrase, should_score) in phrases_by_score {
            /* Iterate over potential phrases */

            /*
             * Drop scanning if the total score dropped below the cutoff*leader
             * and we already found an entry with low enough distance.
             */
            if best_distance == 0 && phrase_heatmap.total_score < query.scan_cutoff * heatmap.max_score {
                // If the score is too low - it won't grow.
                break;
            }

            /* Iterate over tokens by decreasing trigram score until first with
             * an acceptable distance is found */
            let valid_token = phrase_heatmap.tokens
                .iter()
                .map(|(&token_idx, &token_score)| {
                    (token_score, &phrase.tokens[token_idx as usize])
                })
                .sorted_by(|(score_a, token_a), (score_b, token_b)| {
                    /* Prefer shortest for a given score */
                    /* TODO: Maybe score could be divided by token length */
                    let side_a = (score_a, token_b.len());
                    let side_b = (score_b, token_a.len());
                    side_b.partial_cmp(&side_a).expect("Some token score was NaN, it should never be.")
                })
                .map(|(token_score, token)| {
                    let distance = utils::distance(token, &query.must);
                    (token, token_score, distance)
                }).find(|(_token, _score, distance)| {
                    *distance <= max_distance
                });

            if let Some((token, token_score, distance)) = valid_token {
                /* Add result based on best token matching this phrase (lowest
                 * distance, highest score) */

                results.push(
                    Result {
                        origin: &phrase.origin,
                        index: phrase.idx,
                        score: token_score,
                        should_score,
                        token,
                        distance,
                    });

                best_distance = std::cmp::min(distance, best_distance);

                /*
                 * Early break if:
                 * - we reached the limit,
                 * - we already have "good enough" result by the distance metric.
                 */
               if best_distance == 0 && results.len() >= limit {
                    break;
               }
            }
        }

        results.sort_unstable_by(|a, b| {
            let side_a = (a.distance, -a.score, -a.should_score, a.origin.len());
            let side_b = (b.distance, -b.score, -b.should_score, b.origin.len());
            side_a.partial_cmp(&side_b).unwrap_or(Ordering::Equal)
        });

        results.truncate(limit);
        results
    }

    pub fn search(&self, query: &Query) -> Vec<Result> {
        let heatmap = self.create_heatmap(&query.must);
        let should_scores = self.should_scores(&heatmap, &query.should);
        self.filtered_results(query, &heatmap, should_scores)
    }
}
