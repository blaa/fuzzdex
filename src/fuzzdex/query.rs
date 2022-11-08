use crate::utils;

#[derive(Debug)]
pub struct Query {
    /// Token that must match with given maximal distance
    pub must: String,
    /// Optional `should` tokens that increase phrase score so it has higher
    /// probability of fitting within the `limit`.
    pub should: Vec<String>,
    /// Optional constraints that must match.
    /// TODO: This could support a HashSet of various constraints (ORed)
    pub constraint: Option<usize>,
    /// Limit result count. Scanning can be faster with low limit.
    pub limit: Option<usize>,
    /// Max levenshtein distance for "must" token to be a valid result.
    pub max_distance: Option<usize>,
    /// Cutoff phrase scanning when it's score is < `cutoff*max_score`.
    pub scan_cutoff: f32,
}

impl Query {
    /// Create a Query with must/should tokens. You should tokenize things and
    /// pass a single token, but if the internal tokenizer splits must into many
    /// tokens, the longest will be set as a `must` and others moved to
    /// `should`.
    pub fn new(must: &str, should: &[&str]) -> Self {
        let mut should_tokens: Vec<String> = should.iter().map(|s| s.to_string()).collect();

        /* Sometimes must token passed in query is not tokenized in the same way we do */
        let mut tokens: Vec<String> = utils::tokenize(must, 1);
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
            scan_cutoff: 0.3,
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

    pub fn scan_cutoff(mut self, cutoff: f32) -> Self {
        self.scan_cutoff = cutoff;
        self
    }
}
