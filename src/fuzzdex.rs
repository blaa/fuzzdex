use std::collections::HashMap;
use std::collections::HashSet;

/* Fast hashing, but requires AES-ni extensions */
type FastHash = ahash::RandomState;

pub mod query;
pub mod indexer;
pub mod seeker;

#[cfg(test)]
mod tests;

/// Token owning a trigram is uniquely identified by phrase index + token index.
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
pub struct Indexer {
    /// Trigram entries: {"abc": TrigramEntry, "cde": ...}.
    db: HashMap<String, TrigramEntry, FastHash>,

    /// Phrase metadata.
    phrases: HashMap<usize, PhraseEntry, FastHash>
}

