use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;
use std::sync::Mutex;

use regex::Regex;
use lazy_static::lazy_static;
use unicode_segmentation::UnicodeSegmentation;
use unicode_normalization::UnicodeNormalization;
use unicode_categories::UnicodeCategories;
use lru::LruCache;

/* Fast hashing, but requires AES-ni extensions */
type FastHash = ahash::RandomState;

pub fn trigramize(token: &str) -> Vec<String> {
    /* NOTE: Maybe accent removal should be done during tokenization? That makes
     * edit distance ignore accents though */

    /* Normalize accents as separate unicode characters and filter them out */
    let token: String = token.nfd().filter(|ch| !ch.is_mark_nonspacing()).collect();

    /* Unicode characters start at various byte boundaries */
    let graphemes: Vec<&str> = token.graphemes(true).collect::<Vec<&str>>();
    let cnt = graphemes.len();
    if cnt < 3 {
        /* Could be longer in bytes, but it has only 1 grapheme */
        return Vec::new();
    }

    let mut trigrams: Vec<String> = Vec::from_iter(
        (0..graphemes.len() - 2).map(|i| &graphemes[i..i + 3]).map(|s| s.join(""))
    );

    /* Reduce errors on short strings */
    match cnt {
        4 | 5 => {
            trigrams.push(graphemes[0].to_string() + graphemes[1] + graphemes[cnt - 1]);
            trigrams.push(graphemes[0].to_string() + graphemes[cnt - 2] + graphemes[cnt - 1]);
        }
        _ => {}
    }
    trigrams
}

lazy_static! {
    static ref SEPARATOR: Regex = Regex::new("[- \t\n'\"_.,]+").expect("invalid regexp");
}

/* Should this be Vec, or maybe hashset? What about non-unique tokens? */
pub fn tokenize(phrase: &str, min_length: usize) -> Vec<String> {
    let tokens = SEPARATOR.split(phrase)
        .into_iter()
        .map(|t| t.trim().to_lowercase())
        .filter(|t| t.len() >= min_length)
        .collect();
    tokens
}

#[derive(Debug)]
pub struct Query {
    must: String,
    should: Vec<String>,
    /* TODO: This could support a HashSet of various constraints (ORed) */
    constraint: Option<usize>,
    limit: Option<usize>,
    /* Max levenhstein distance for "must" token to be a valid result */
    max_distance: Option<usize>,
}

impl Query {
    pub fn new(must: &str, should: &[&str]) -> Self {
        let mut should_tokens: Vec<String> = should.iter().map(|s| s.to_string()).collect();

        /* Sometimes must token passed in query is not tokenized in the same way we do */
        let mut tokens: Vec<String> = tokenize(must, 2);
        let must_token: String = if tokens.len() > 1 {
            tokens.sort_unstable_by_key(|token| - (token.len() as i64));
            for token in tokens[1..].iter() {
                should_tokens.push(token.to_owned());
            }
            tokens[0].to_owned()
        } else {
            must.to_string()
        };

        Self {
            must: must_token,
            should: should_tokens,
            constraint: None,
            limit: None,
            max_distance: Some(2),
        }
    }

    pub fn constraint(mut self, constraint: Option<usize>) -> Self {
        self.constraint = constraint;
        self
    }

    pub fn max_distance(mut self, max_distance: Option<usize>) -> Self {
        self.max_distance = max_distance;
        self
    }

    pub fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }
}

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
    score: HashMap<usize, HashMap<u16, f32>, FastHash>,
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
        for trigram in trigramize(token) {
            let entry = self.db.entry(trigram).or_insert(
                TrigramEntry { positions: Vec::new(), score: 0.0 }
            );
            entry.positions.push(Position { phrase_idx, token_idx });
        }
    }

    /* Add a phrase mapped to an index. Phrase can be found by one of it's fuzzy-matched tokens */
    pub fn add_phrase(&mut self, phrase: &str, phrase_idx: usize,
                      constraints: Option<&HashSet<usize, FastHash>>) {
        let phrase_tokens = tokenize(phrase, 3);
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

        for trigram in trigramize(token) {
            if let Some(entry) = db.get(&trigram) {
                for position in entry.positions.iter() {
                    let by_token = heatmap.score.entry(position.phrase_idx).or_insert_with(HashMap::new);
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
            let mut trigrams = trigramize(token);
            /* Use only first 4 trigrams for should scores */
            trigrams.truncate(3);
            for trigram in trigramize(token) {
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

        /* FIXME: This should be done once in one place. I'm unsure if it's ok */
        let must_graphemes = query.must.graphemes(true).collect::<Vec<&str>>();

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
                        let token_graphemes = token.graphemes(true).collect::<Vec<&str>>();
                        let (distance, _) = levenshtein_diff::distance(&token_graphemes,
                                                                       &must_graphemes);
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
    fn it_tokenizes() {
        let tokens: Vec<String> = tokenize("This are b some-Words.", 2);
        println!("Tokenized into {:?}", tokens);
        for token in ["this", "some", "words"].iter() {
            println!("Testing {}", token);
            assert!(tokens.contains(&token.to_string()));
        }
        assert!(!tokens.contains(&"b".to_string()));
    }

    #[test]
    fn it_trigramizes() {
        let testcases = [
            ("newyork", ["new", "ewy", "wyo", "yor", "ork"].to_vec()),
            ("kлаус", ["kла", "лау", "аус"].to_vec()),
            ("newyor", ["new", "ewy", "wyo", "yor"].to_vec()),
            ("ewyor", ["ewy", "wyo", "yor"].to_vec()),
            ("łódź", ["łod", "odz", "łdz", "łoz"].to_vec()),
            ("y̆es", ["yes"].to_vec()),
        ];
        for (input, proper_trigrams) in testcases.iter() {
            let trigrams: Vec<String> = trigramize(input);
            println!("Trigramized {} into {:?}", input, trigrams);
            for trigram in proper_trigrams.iter() {
                println!("Testing {}", trigram);
                assert!(trigrams.contains(&trigram.to_string()));
            }
        }
    }

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
