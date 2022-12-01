use std::collections::HashMap;
use super::FastHash;

/* Trigram heatmap is a partial query result */
#[derive(Debug, Clone)]
pub struct PhraseHeatmap {
    /// Phrase Index
    pub phrase_idx: usize,
    /// Token trigram score: token_idx -> score
    pub tokens: HashMap<u32, f32, FastHash>,
    /// Total phrase score
    pub total_score: f32,
}

impl PhraseHeatmap {
    pub fn new(phrase_idx: usize) -> PhraseHeatmap {
        PhraseHeatmap {
            phrase_idx,
            tokens: HashMap::with_hasher(FastHash::new()),
            total_score: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Heatmap {
    /* Trigram score */
    /* phrase_idx -> token_idx -> score */
    pub phrases: HashMap<usize, PhraseHeatmap, FastHash>,
    /* Max phrase score */
    pub max_score: f32,
}

impl Heatmap {
    pub fn new() -> Heatmap {
        Heatmap {
            phrases: HashMap::with_capacity_and_hasher(8, FastHash::new()),
            max_score: 0.0,
        }
    }

    pub fn add_phrase(&mut self, phrase_idx: usize, token_idx: u32, score: f32) {
        let phrase_level = self.phrases.entry(phrase_idx)
            .or_insert_with(|| PhraseHeatmap::new(phrase_idx));

        /* Get or create token-level entry */
        let token_score = phrase_level.tokens.entry(token_idx).or_insert(0.0);
        *token_score += score;

        phrase_level.total_score += score;
        if phrase_level.total_score > self.max_score {
            self.max_score = phrase_level.total_score;
        }
    }

    pub fn len_phrases(&self) -> usize {
        self.phrases.len()
    }

    pub fn has_phrase(&self, phrase_idx: usize) -> bool {
        self.phrases.contains_key(&phrase_idx)
    }
}
