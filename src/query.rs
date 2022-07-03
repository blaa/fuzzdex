use super::utils;

#[derive(Debug)]
pub struct Query {
    pub must: String,
    pub should: Vec<String>,
    /* TODO: This could support a HashSet of various constraints (ORed) */
    pub constraint: Option<usize>,
    pub limit: Option<usize>,
    /* Max levenhstein distance for "must" token to be a valid result */
    pub max_distance: Option<usize>,
}

impl Query {
    pub fn new(must: &str, should: &[&str]) -> Self {
        let mut should_tokens: Vec<String> = should.iter().map(|s| s.to_string()).collect();

        /* Sometimes must token passed in query is not tokenized in the same way we do */
        let mut tokens: Vec<String> = utils::tokenize(must, 2);
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
