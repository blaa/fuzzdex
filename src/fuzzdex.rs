use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use itertools::Itertools;

use lru::LruCache;
use super::utils;
use super::query::Query;

/* Fast hashing, but requires AES-ni extensions */
type FastHash = ahash::RandomState;


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
    /* Token trigram score */
    tokens: HashMap<u16, f32, FastHash>,
    /* Total phrase score */
    total_score: f32,
}

impl PhraseHeatmap {
    fn new() -> PhraseHeatmap {
        PhraseHeatmap {
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

#[derive(Debug)]
struct Position {
    /// Phrase index / value
    phrase_idx: usize,
    /// Token within phrase
    token_idx: u16,
}

/// Trigram data inside the Index
#[derive(Debug)]
struct TrigramEntry {
    /// Where trigram appears (phrase / token).
    positions: Vec<Position>,
    /// Trigram score; the more unique trigram, the higher score.
    score: f32,
}

/// Information stored about the inserted phrase
#[derive(Debug)]
struct PhraseEntry {
    /// Original phrase.
    origin: String,
    /// Tokens that build this phrase.
    tokens: Vec<String>,
    /// Constraints with which this phrase is valid.
    constraints: HashSet<usize, FastHash>
}

/// Initial Index instance that can gather entries, but can't be queried.
#[derive(Debug)]
pub struct Index {
    /// Trigram entries.
    db: HashMap<String, TrigramEntry, FastHash>,

    /// Phrase metadata.
    phrases: HashMap<usize, PhraseEntry, FastHash>,

    /// LRU cache of must tokens.
    cache: Mutex<LruCache<String, Arc<Heatmap>, FastHash>>,
}

/// Produced by Index::finish() and can be queried.
pub struct IndexReady(Index);

impl Index {
    /// Create new empty fuzzdex.
    pub fn new() -> Index {
        Index {
            db: HashMap::with_capacity_and_hasher(32768, FastHash::new()),
            phrases: HashMap::with_hasher(FastHash::new()),
            cache: Mutex::new(LruCache::with_hasher(30000, FastHash::new())),
        }
    }

    fn add_token(&mut self, token: &str, phrase_idx: usize, token_idx: u16) {
        for trigram in utils::trigramize(token) {
            let entry = self.db.entry(trigram).or_insert(
                TrigramEntry { positions: Vec::new(), score: 0.0 }
            );
            entry.positions.push(Position { phrase_idx, token_idx });
        }
    }

    /// Add a phrase mapped to an index. Phrase can be found by one of it's fuzzy-matched tokens.
    pub fn add_phrase(&mut self, phrase: &str, phrase_idx: usize,
                      constraints: Option<&HashSet<usize, FastHash>>) {
        let phrase_tokens = utils::tokenize(phrase, 3);
        for (token_idx, token) in phrase_tokens.iter().enumerate() {
            if token.len() < 2 {
                continue;
            }
            self.add_token(token, phrase_idx, token_idx as u16);
        }
        let constraints = match constraints {
            Some(constraints) => constraints.clone(),
            None => HashSet::with_hasher(FastHash::new())
        };

        self.phrases.insert(phrase_idx, PhraseEntry {
            origin: phrase.to_string(),
            tokens: phrase_tokens,
            constraints,
        });
    }

    /// Consume original Index and return IndexReady class with querying ability.
    pub fn finish(mut self) -> IndexReady {
        if self.db.is_empty() {
            return IndexReady(self);
        }
        /* Can certainly be faster; can it be less verbose though? */
        let lengths: Vec<usize> = self.db.values().map(|v| v.positions.len()).collect();
        let max = *lengths.iter().max().unwrap_or(&1) as f32;
        let min = *lengths.iter().min().unwrap_or(&0) as f32;
        let range: f32 = max - min;
        let coeff: f32 = 1.0 / (range + 0.00001);
        for (_, entry) in self.db.iter_mut() {
            entry.score = (max + 1.0 - (entry.positions.len() as f32)) * coeff;
        }
        IndexReady(self)
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexReady {

    /// Create trigram heatmap for a given token.
    fn create_heatmap(&self, token: &str) -> Arc<Heatmap> {
        let index = &self.0;
        let db = &index.db;

        /* LRU cache updates position even on get and needs mutable reference */
        {
            let mut cache = index.cache.lock().unwrap();
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
                        PhraseHeatmap::new);

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
            let mut cache = index.cache.lock().unwrap();
            cache.put(token.to_string(), heatmap.clone());
        }
        heatmap
    }

    fn should_scores(&self, heatmap: &Heatmap, should_tokens: &[String])
                     -> HashMap<usize, f32, FastHash> {
        let mut map: HashMap<usize, f32, FastHash> = HashMap::with_capacity_and_hasher(
            heatmap.phrases.len(), FastHash::new()
        );
        let db = &self.0.db;

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
        let index = &self.0;
        let max_distance: usize = query.max_distance.unwrap_or(usize::MAX);
        let limit: usize = query.limit.unwrap_or(usize::MAX);

        let phrases_by_score = heatmap.phrases
            .iter()
            .map(|(idx, heatmap)| {
                let phrase = &index.phrases[idx];
                (idx, heatmap, phrase, *should_scores.get(idx).unwrap_or(&0.0))
            })
            .filter(|(_, _, phrase, _)| {
                if let Some(constraint) = query.constraint {
                    phrase.constraints.contains(&constraint)
                } else {
                    /* No constraint - return all */
                    true
                }
            })
            .sorted_by(|(_, heat_a, phrase_a, should_a), (_, heat_b, phrase_b, should_b)| {
                /* Sort by score and then by a should score; for identical - prefer shortest. */
                (heat_b.total_score, should_b, phrase_a.origin.len()).partial_cmp(
                    &(heat_a.total_score, should_a, phrase_b.origin.len())).unwrap()
            });

        for (phrase_idx, phrase_heatmap, phrase, should_score) in phrases_by_score {
            /* Iterate over potential phrases */

            /* Drop scanning if the total score dropped below the cutoff*leader. */
            if phrase_heatmap.total_score < query.scan_cutoff * heatmap.max_score {
                // If the score is too low - it won't grow.
                break;
            }

            /* Iterate over tokens by decreasing trigram score until first matching is found */
            let valid_token = phrase_heatmap.tokens
                .iter()
                .map(|(&idx, &score)|
                     (score, &phrase.tokens[idx as usize]))
                .sorted_by(|(score_a, token_a), (score_b, token_b)| {
                    /* Prefer shortest for a given score */
                    /* TODO: Maybe score could be divided by token length */
                    let side_a = (score_a, token_b.len());
                    let side_b = (score_b, token_a.len());
                    side_b.partial_cmp(&side_a).unwrap()
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
                        index: *phrase_idx,
                        score: token_score,
                        should_score,
                        token,
                        distance,
                    });

                /* Early break if we reached limit */
                if results.len() >= limit {
                    break;
                }
            }
        }

        results.sort_unstable_by(|a, b| {
            let side_a = (a.distance, -a.score, -a.should_score, a.origin.len());
            let side_b = (b.distance, -b.score, -b.should_score, b.origin.len());
            side_a.partial_cmp(&side_b).unwrap()
        });

        results
    }

    pub fn search(&self, query: &Query) -> Vec<Result> {
        let heatmap = self.create_heatmap(&query.must);
        let should_scores = self.should_scores(&heatmap, &query.should);
        self.filtered_results(query, &heatmap, should_scores)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut idx = super::Index::new();
        let mut constraints: HashSet<usize, FastHash> = HashSet::with_hasher(FastHash::new());
        constraints.insert(1);

        idx.add_phrase("This is an entry", 1, None);
        idx.add_phrase("Another entry entered.", 2, Some(&constraints));
        idx.add_phrase("Another about the testing.", 3, None);
        idx.add_phrase("Tester tested a test suite.", 4, None);
        let idx = idx.finish();

        /* First query */
        let query = Query::new("another", &["testing"]).limit(Some(60));
        println!("Querying {:?}", query);
        let results = idx.search(&query);

        for result in &results {
            println!("Got result {:?}", result);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].index, 3);
        assert_eq!(results[1].index, 2);
        assert!(results[0].should_score > results[1].should_score,
                "First result should have higher score than second one");

        /* Test constraint */
        let query = Query::new("another", &["testing"])
            .constraint(Some(1));
        println!("Querying {:?}", query);
        let results = idx.search(&query);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 2);

        /* Third query */
        let query = Query::new("this", &["entry"]).limit(Some(60));
        let results = idx.search(&query);

        for result in &results {
            println!("Got result {:?}", result);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);
        assert!(results[0].should_score > 0.0, "First result should have non-zero should-score");

        /* Test multiple tokens matching in single phrase */
        let query = Query::new("test", &[]).limit(Some(60));
        println!("Querying {:?}", query);
        let results = idx.search(&query);

        for result in &results {
            println!("Got result {:?}", result);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 4);
    }
}
