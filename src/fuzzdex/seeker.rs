use std::collections::HashMap;
use std::cmp::Ordering;
// sorted_by
use itertools::Itertools;

use std::sync::Arc;
use std::sync::Mutex;
use lru::LruCache;

use crate::utils;
use super::query::Query;
use super::{Indexer, FastHash};

mod heatmap;
use heatmap::Heatmap;

/// Query result
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult<'a> {
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

#[derive(Clone, Default, Debug)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    /// Current size of the cache, calculated on request.
    pub size: usize,
}

struct Cache {
    stats: CacheStats,
    heatmaps: LruCache<String, Arc<Heatmap>, FastHash>,
}

/// Produced by Index::finish() and can be queried.
pub struct Index {
    /// Index prepared for querying.
    pub index: Indexer,

    /// LRU cache of must tokens.
    cache: Mutex<Cache>,
}

impl Index {
    /// Create new searchable index with a given cache size.
    pub fn new(indexer: Indexer, cache_size: usize) -> Index {
        let cache = Cache {
            stats: CacheStats::default(),
            heatmaps: LruCache::with_hasher(cache_size, FastHash::new()),
        };
        Index {
            index: indexer,
            cache: Mutex::new(cache),
        }
    }

    /// Create a trigram heatmap for a given token.
    fn create_heatmap(&self, token: &str) -> Arc<Heatmap> {
        let index = &self.index;
        let db = &index.db;

        /* LRU cache updates position even on get and needs mutable reference */
        {
            let mut cache = self.cache.lock().unwrap();
            let heatmap = cache.heatmaps.get(token).cloned();
            if let Some(heatmap) = heatmap {
                /* We operate on reference-counted heatmaps to eliminate unnecessary copying */
                cache.stats.hits += 1;
                return heatmap;
            }
            cache.stats.misses += 1;
        }

        let mut heatmap = Heatmap::new();

        for trigram in utils::trigramize(token) {
            if let Some(entry) = db.get(&trigram) {
                for position in entry.positions.iter() {
                    heatmap.add_phrase(position.phrase_idx, position.token_idx, entry.score);
                }
            }
        }

        let heatmap = Arc::new(heatmap);
        {
            let mut cache = self.cache.lock().unwrap();
            cache.heatmaps.put(token.to_string(), heatmap.clone());
            cache.stats.inserts += 1;
        }
        heatmap
    }

    fn should_scores(&self, heatmap: &Heatmap, should_tokens: &[String],
                     constraint: Option<usize>)
                     -> HashMap<usize, f32, FastHash> {
        let mut map: HashMap<usize, f32, FastHash> = HashMap::with_capacity_and_hasher(
            heatmap.len_phrases(), FastHash::new()
        );
        let db = &self.index.db;

        for token in should_tokens {
            let mut trigrams = utils::trigramize(token);
            /* Use only first 4 trigrams for should scores. This has to effects:
             * - Improves speed for long words.
             * - Reduces impact of should score on ordering during final pass.
             */
            trigrams.truncate(4);
            for trigram in trigrams {
                if let Some(entry) = db.get(&trigram) {
                    for position in entry.positions.iter() {
                        // Ignore scores from phrases that don't match constraint.
                        if let Some(constraint_id) = constraint {
                            let phrase_entry = self.index.phrases.get(&position.phrase_idx).unwrap();
                            if !phrase_entry.constraints.contains(&constraint_id) {
                                // Ignore score from this phrase.
                                continue;
                            }
                        }

                        if heatmap.has_phrase(position.phrase_idx) {
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
                        should_scores: HashMap<usize, f32, FastHash>) -> Vec<SearchResult> {
        let mut results: Vec<SearchResult> = Vec::with_capacity(query.limit.unwrap_or(3));
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
                /* The sorted data are scanned and when fuzzdex is happy with the result
                 * will stop scanning. Sorting impacts the behaviour of this early break.
                 *
                 * Sort by a combined score, and prefer shortest solutions if
                 * score is equal. The early break triggers only if the must
                 * token matches perfectly. With sorting by must-token score
                 * only, it could miss good solutions.
                 */
                let side_a = (heat_b.total_score + should_b, phrase_a.origin.len());
                let side_b = (heat_a.total_score + should_a, phrase_b.origin.len());
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

            /* Iterate over tokens inside this phrase by decreasing trigram
             * score until the first with an acceptable distance is found */
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
                    SearchResult {
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
                 * - we already have "good enough" result by the distance metric,
                 * - we have considered solution with best must+should score.
                 */
               if best_distance == 0 && results.len() >= limit {
                   break;
               }
            }
        }

        results.sort_unstable_by(|a, b| {
            let side_a = (a.distance, -a.score, -a.should_score, a.origin.len(), &a.origin);
            let side_b = (b.distance, -b.score, -b.should_score, b.origin.len(), &b.origin);
            side_a.partial_cmp(&side_b).unwrap_or(Ordering::Equal)
        });

        results.truncate(limit);
        results
    }

    pub fn search(&self, query: &Query) -> Vec<SearchResult> {
        let heatmap = self.create_heatmap(&query.must);
        let should_scores = self.should_scores(&heatmap, &query.should, query.constraint);
        self.filtered_results(query, &heatmap, should_scores)
    }

    pub fn cache_stats(&self) -> CacheStats {
        let cache = self.cache.lock().unwrap();
        let mut stats = cache.stats.clone();
        stats.size = cache.heatmaps.len();
        stats
    }
}
