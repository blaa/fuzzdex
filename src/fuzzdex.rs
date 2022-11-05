use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::cmp::Ordering;
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

#[derive(Debug)]
struct Position {
    /// Phrase index / value
    phrase_idx: usize,
    /// Token within phrase (first position in case multiple exist)
    token_idx: u32,
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
    /// Phrase index, as given by the user.
    idx: usize,
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
    /// Trigram entries: {"abc": TrigramEntry, "cde": ...}.
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

    /// Consume original Index and return IndexReady class with querying ability.
    pub fn finish(mut self) -> IndexReady {
        if self.db.is_empty() {
            return IndexReady(self);
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
        IndexReady(self)
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexReady {

    /// Create a trigram heatmap for a given token.
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

    #[test]
    fn it_works_with_case_change_and_spellerror() {
        let mut idx = super::Index::new();

        idx.add_phrase("Warszawa", 1, None);
        idx.add_phrase("Rakszawa", 2, None);
        /* "asz" trigram will appear during a spelling error in wa(r)szawa */
        idx.add_phrase("Waszeta", 3, None);
        idx.add_phrase("Waszki", 4, None);
        idx.add_phrase("Kwaszyn", 5, None);
        idx.add_phrase("Jakszawa", 6, None);
        idx.add_phrase("Warszew", 7, None);
        let idx = idx.finish();

        /* Query with lowercase and single spell error */
        let query = Query::new("waszawa", &[]).limit(Some(1));
        println!("Querying {:?}", query);
        let results = idx.search(&query);

        for result in &results {
            println!("Got result {:?}", result);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);
    }

    /// Street names often contain single digits that should correctly
    /// be used in "should" statements.
    #[test]
    fn it_works_with_small_tokens() {

        let mut idx = super::Index::new();

        idx.add_phrase("1 May", 1, None);
        idx.add_phrase("2 May", 2, None);
        idx.add_phrase("3 May", 3, None);
        idx.add_phrase("4 July", 4, None);
        let idx = idx.finish();

        /* First query */
        let query = Query::new("may", &["1"]).limit(Some(1));
        println!("Querying {:?}", query);
        let results = idx.search(&query);
        for result in &results {
            println!("Got result {:?}", result);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);

        let query = Query::new("may", &["2"]).limit(Some(1));
        println!("Querying {:?}", query);
        let results = idx.search(&query);
        for result in &results {
            println!("Got result {:?}", result);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 2);

        let query = Query::new("may", &["3"]).limit(Some(1));
        println!("Querying {:?}", query);
        let results = idx.search(&query);
        for result in &results {
            println!("Got result {:?}", result);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 3);
    }

    #[test]
    fn it_behaves_with_repeating_patterns() {
        let mut idx = super::Index::new();

        let repeating_phrase = "abcaBC";
        idx.add_phrase(&repeating_phrase, 1, None);
        let idx = idx.finish();

        /* Should generate only three trigrams: abc, bca, cab */
        assert_eq!(3, idx.0.db.len());
        assert!(idx.0.db.contains_key("abc"));
        assert!(idx.0.db.contains_key("bca"));
        assert!(idx.0.db.contains_key("cab"));

        let query = Query::new("abc", &[]).max_distance(Some(3)).limit(Some(3));
        let results = idx.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);
        assert_eq!(results[0].distance, 3);

        /* Similar but duplicates in separate tokens */
        let mut idx = super::Index::new();
        let repeating_phrase = "abcx uabc";
        idx.add_phrase(&repeating_phrase, 1, None);
        let idx = idx.finish();

        let query = Query::new("abc", &[]).limit(Some(3));
        let results = idx.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);
        assert_eq!(results[0].distance, 1);
    }

    #[test]
    fn it_behaves_with_too_long_inputs() {
        let mut idx = super::Index::new();

        /* Single token, multiple duplicated trigrams */
        let long_string = "abc".repeat(1000);
        idx.add_phrase(&long_string, 1, None);
        let idx = idx.finish();

        /* Generates 3 different trigrams */
        assert_eq!(3, idx.0.db.len());
        assert!(idx.0.db.contains_key("abc"));
        assert!(idx.0.db.contains_key("bca"));
        assert!(idx.0.db.contains_key("cab"));

        println!("Added {}", long_string);
        let query = Query::new(&long_string, &[]).limit(Some(3));
        let results = idx.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);

        /* A lot of small tokens */
        let mut idx = super::Index::new();
        let long_string = "abc ".repeat(70000);
        idx.add_phrase(&long_string, 1, None);
        let idx = idx.finish();

        /* Generates only one trigram */
        assert_eq!(1, idx.0.db.len());
        assert!(idx.0.db.contains_key("abc"));

        let query = Query::new("abc", &[]).limit(Some(3));
        let results = idx.search(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].index, 1);
        assert_eq!(results[0].distance, 0);
    }
}
