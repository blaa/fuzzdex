use std::iter::FromIterator;
use regex::Regex;
use lazy_static::lazy_static;

use unicode_segmentation::UnicodeSegmentation;
use unicode_normalization::UnicodeNormalization;
use unicode_categories::UnicodeCategories;

lazy_static! {
    /* NOTE: Maybe detect a unicode group for interpunction chars */
    static ref SEPARATOR: Regex = Regex::new("[- \t\n'’`„\"_.,;:=]+").expect("invalid regexp");
}

pub fn trigramize(token: &str) -> Vec<String> {
    /* NOTE: Maybe accent removal should be done during tokenization? That makes
     * edit distance ignore accents though */

    /* Normalize accents as separate unicode characters and filter them out */
    let mut token: String = token.nfd().filter(|ch| !ch.is_mark_nonspacing()).collect();

    /* NOTE: Various language-specific letters. It's not required, but can
     * handle certain human errors better */
    for (ch_from, ch_to) in [("ł", "l"), ("ß", "ss")] {
        token = token.replace(ch_from, ch_to);
    }

    /* Unicode characters start at various byte boundaries */
    let graphemes: Vec<&str> = token.graphemes(true).collect::<Vec<&str>>();
    let cnt = graphemes.len();

    let mut trigrams: Vec<String> = if cnt >= 3 {
        Vec::from_iter(
            (0..graphemes.len() - 2).map(|i| &graphemes[i..i + 3]).map(|s| s.join(""))
        )
    } else {
        Vec::new()
    };

    match cnt {
        /* Generate pseudo trigrams for 1 and 2 letter words. No typo-tolerance. */
        1 => {
            trigrams.push(graphemes[0].to_string() + "  ");
        }
        2 => {
            trigrams.push(graphemes[0].to_string() + graphemes[1] + " ");
        }
        /* Increase typo-tolerance on short strings */
        4 | 5 => {
            trigrams.push(graphemes[0].to_string() + graphemes[1] + graphemes[cnt - 1]);
            trigrams.push(graphemes[0].to_string() + graphemes[cnt - 2] + graphemes[cnt - 1]);
        }
        _ => {}
    }
    trigrams
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

/** Compare first 500 graphemes of strings and return a Levenshtein distance */
pub fn distance(side_a: &str, side_b: &str) -> usize {
    /* Levenshtein algorithm is recursive and will fail with too long tokens.
     * Limit comparison to first X graphemes to eliminate possible DoS attacks.
     * Tokens should be "words" anyway. Maybe return Result instead? */

    let graphemes_a = side_a.graphemes(true).take(500).collect::<Vec<&str>>();
    let graphemes_b = side_b.graphemes(true).take(500).collect::<Vec<&str>>();
    /* By default levenshtein module uses _memoization version. Tabulation seems
     * faster in my tests */
    let (distance, _) = levenshtein_diff::levenshtein_tabulation(&graphemes_a, &graphemes_b);
    distance
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_tokenizes() {
        let tokens: Vec<String> = tokenize("This are b some-Words.", 2);
        println!("Tokenized into {:?}", tokens);
        for token in ["this", "are", "some", "words"].iter() {
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
            ("łódź", ["lod", "odz", "ldz", "loz"].to_vec()),
            ("y̆es", ["yes"].to_vec()),
            ("12", ["12 "].to_vec()),
            ("1", ["1  "].to_vec()),
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
}
