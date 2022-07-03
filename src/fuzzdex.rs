use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use lru::LruCache;
use super::utils;
use super::query::Query;

/* Fast hashing, but requires AES-ni extensions */
type FastHash = ahash::RandomState;


/* Query result */
#[derive(Debug, Clone, PartialEq)]
pub struct Result<'a> {
    /* Original phrase before tokenization */
    pub origin: &'a str,
    /* Returned index, a "value" */
    pub index: usize,
    /* Token that matched the must token */
    pub token: &'a str,
    /* Token distance to the query */
    pub distance: usize,
    /* Trigram score of the phrase */
    pub score: f32,
    /* Bonus score from /should/ tokens */
    pub should_score: f32,
}

/* TODO Maybe instead of cloning - use Rc<>? */
#[derive(Debug, Clone)]
struct Heatmap {
    /* Trigram score */
    /* phrase_idx -> token_idx -> score */
    score: HashMap<usize, HashMap<u16, f32, FastHash>, FastHash>,
    max: f32,
}

#[derive(Debug)]
struct Position {
    /* Phrase index / value */
    phrase_idx: usize,
    /* Token within phrase */
    token_idx: u16,
}

/* Trigram data inside the Index */
#[derive(Debug)]
struct TrigramEntry {
    /* Where trigram appears (phrase / token) */
    positions: Vec<Position>,
    /* Trigram score; the more unique trigram, the higher score */
    score: f32,
}

/* Information stored about the inserted phrase */
#[derive(Debug)]
struct PhraseEntry {
    /* Original phrase */
    origin: String,
    /* Tokens that build this phrase */
    tokens: Vec<String>,
    /* Constraints with which this phrase is valid */
    constraints: HashSet<usize, FastHash>
}

/* Initial Index instance that can gather entries, but can't be queried */
#[derive(Debug)]
pub struct Index {
    /* Trigram entries */
    db: HashMap<String, TrigramEntry, FastHash>,

    /* Phrase metadata */
    phrases: HashMap<usize, PhraseEntry, FastHash>,

    /* LRU cache of must tokens */
    cache: Mutex<LruCache<String, Arc<Heatmap>>>,
}

/* Produced by Index::finish() and can be queried */
pub struct IndexReady(Index);

impl Index {
    pub fn new() -> Index {
        Index {
            db: HashMap::with_capacity_and_hasher(32768, FastHash::new()),
            phrases: HashMap::with_hasher(FastHash::new()),
            cache: Mutex::new(LruCache::new(30000)),
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

    /* Add a phrase mapped to an index. Phrase can be found by one of it's fuzzy-matched tokens */
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

    /* Consume original Index and return IndexReady class with querying ability */
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


impl IndexReady {

    /* Create trigram heatmap for a given token */
    fn create_heatmap(&self, token: &str) -> Arc<Heatmap> {
        let db = &self.0.db;

        /* LRU cache updates position even on get and needs mutable reference */
        {
            let mut cache = self.0.cache.lock().unwrap();
            if let Some(heatmap) = cache.get(token) {
                /* We operate on reference-counted heatmaps to eliminate unnecessary copying */
                return heatmap.clone();
            }
        }

        let mut heatmap = Heatmap {
            score: HashMap::with_capacity_and_hasher(1024, FastHash::new()),
            max: 0.0,
        };

        for trigram in utils::trigramize(token) {
            if let Some(entry) = db.get(&trigram) {
                for position in entry.positions.iter() {
                    let by_token = heatmap.score.entry(position.phrase_idx).or_insert_with(
                        || HashMap::with_capacity_and_hasher(32, FastHash::new()));
                    let token_score = by_token.entry(position.token_idx).or_insert(0.0);
                    *token_score += entry.score;

                    if *token_score > heatmap.max {
                        heatmap.max = *token_score;
                    }
                }
            }
        }

        let heatmap = Arc::new(heatmap);
        {
            let mut cache = self.0.cache.lock().unwrap();
            cache.put(token.to_string(), heatmap.clone());
        }
        heatmap
    }

    fn should_scores(&self, heatmap: &Heatmap, should_tokens: &Vec<String>)
                     -> HashMap<usize, f32, FastHash> {
        let mut map: HashMap<usize, f32, FastHash> = HashMap::with_capacity_and_hasher(heatmap.score.len(),
                                                                                       FastHash::new());
        let db = &self.0.db;

        for token in should_tokens {
            let mut trigrams = utils::trigramize(token);
            /* Use only first 4 trigrams for should scores */
            trigrams.truncate(3);
            for trigram in utils::trigramize(token) {
                if let Some(entry) = db.get(&trigram) {
                    for position in entry.positions.iter() {
                        if heatmap.score.contains_key(&position.phrase_idx) {
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
        let mut results: Vec<Result> = Vec::new();
        if let Some(limit) = query.limit {
            results.reserve(limit);
        }
        let index = &self.0;
        let max_distance: usize = query.max_distance.unwrap_or(100);


        /* TODO: For now, we convert all entries into results, we could stop earlier */
        for (phrase_idx, tokens) in heatmap.score.iter() {
            let phrase = &index.phrases[phrase_idx];
            if let Some(constraint) = query.constraint {
                if !phrase.constraints.contains(&constraint) {
                    continue
                }
            }

            let mut valid_tokens: Vec<(&String, usize, f32)> = tokens.iter()
                /* Cut off low scoring tokens. TODO: Use trigram count */
                .filter(|(_idx, score)| (**score) > 0.4 * heatmap.max)
                /* Measure levenhstein distance if filter is enabled */
                .map(|(idx, score)| {
                    let token = &phrase.tokens[*idx as usize];
                    if query.max_distance.is_some() {
                        let distance = utils::distance(&token, &query.must);
                        (token, distance, *score)
                    } else {
                        (token, 0, *score)
                    }
                })
                /* Drop ones that are too far away */
                .filter(|(_token, distance, _score)| distance <= &max_distance)
                .collect();

            valid_tokens.sort_unstable_by_key(
                /* Solves PartialOrd for floats in a peculiar way. Should be fine though. */
                |(token, distance, score)| (*distance,
                                            - ((*score) * 10000.0) as i64,
                                            (token.len() as i32))
            );

            if !valid_tokens.is_empty() {
                /* Add result based on best token matching this phrase (lowest
                 * distance, highest score) */

                let best = valid_tokens[0];
                let should_score: f32 = *should_scores.get(phrase_idx).unwrap_or(&0.0);
                results.push(
                    Result {
                        origin: &phrase.origin,
                        index: *phrase_idx,
                        token: best.0,
                        distance: best.1,
                        score: best.2,
                        should_score,
                    });
            }
        }

        results.sort_unstable_by_key(|result|
                                     (result.distance,
                                      (- 1000.0 * result.score) as i64,
                                      (- 1000.0 * result.should_score) as i64,
                                      result.origin.len())

        );

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        results
    }

    pub fn search(&self, query: &Query) -> Vec<Result> {
        let heatmap = self.create_heatmap(&query.must);
        let should_scores = self.should_scores(&heatmap, &query.should);
        let results = self.filtered_results(query, &heatmap, should_scores);
        results
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
        idx.add_phrase("Another about testing.", 3, None);
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
    }
}
