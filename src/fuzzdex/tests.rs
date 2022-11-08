use std::collections::HashSet;

use super::FastHash;
use super::Indexer;
use super::query::Query;

#[test]
fn it_works() {
    let mut idx = Indexer::new();
    let mut constraints: HashSet<usize, FastHash> = HashSet::with_hasher(FastHash::new());
    constraints.insert(1);

    idx.add_phrase("This is an entry", 1, None).unwrap();
    idx.add_phrase("Another entry entered.", 2, Some(&constraints)).unwrap();
    idx.add_phrase("Another about the testing.", 3, None).unwrap();
    idx.add_phrase("Tester tested a test suite.", 4, None).unwrap();
    let idx = idx.finish();
    assert_eq!(idx.cache_stats().inserts, 0);

    /* First query */
    let query = Query::new("another", &["testing"]).limit(Some(60));
    println!("Querying {:?}", query);
    let results = idx.search(&query);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].index, 3);
    assert_eq!(results[1].index, 2);
    assert!(results[0].should_score > results[1].should_score,
            "First result should have higher score than second one");

    assert_eq!(idx.cache_stats().hits, 0);
    assert_eq!(idx.cache_stats().misses, 1);
    assert_eq!(idx.cache_stats().inserts, 1);

    /* Test constraint */
    let query = Query::new("another", &["testing"])
        .constraint(Some(1));
    println!("Querying {:?}", query);
    let results = idx.search(&query);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].index, 2);

    /* Asked for the same token */
    assert_eq!(idx.cache_stats().hits, 1);
    assert_eq!(idx.cache_stats().misses, 1);
    assert_eq!(idx.cache_stats().inserts, 1);

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
    let mut idx = super::Indexer::new();

    idx.add_phrase("Warszawa", 1, None).unwrap();
    idx.add_phrase("Rakszawa", 2, None).unwrap();
    /* "asz" trigram will appear during a spelling error in wa(r)szawa */
    idx.add_phrase("Waszeta", 3, None).unwrap();
    idx.add_phrase("Waszki", 4, None).unwrap();
    idx.add_phrase("Kwaszyn", 5, None).unwrap();
    idx.add_phrase("Jakszawa", 6, None).unwrap();
    idx.add_phrase("Warszew", 7, None).unwrap();
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

    let mut idx = super::Indexer::new();

    idx.add_phrase("1 May", 1, None).unwrap();
    idx.add_phrase("2 May", 2, None).unwrap();
    idx.add_phrase("3 May", 3, None).unwrap();
    idx.add_phrase("4 July", 4, None).unwrap();
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
    let mut idx = super::Indexer::new();

    let repeating_phrase = "abcaBC";
    idx.add_phrase(&repeating_phrase, 1, None).unwrap();
    let idx = idx.finish();

    /* Should generate only three trigrams: abc, bca, cab */
    assert_eq!(3, idx.index.db.len());
    assert!(idx.index.db.contains_key("abc"));
    assert!(idx.index.db.contains_key("bca"));
    assert!(idx.index.db.contains_key("cab"));

    let query = Query::new("abc", &[]).max_distance(Some(3)).limit(Some(3));
    let results = idx.search(&query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].index, 1);
    assert_eq!(results[0].distance, 3);

    /* Similar but duplicates in separate tokens */
    let mut idx = super::Indexer::new();
    let repeating_phrase = "abcx uabc";
    idx.add_phrase(&repeating_phrase, 1, None).unwrap();
    let idx = idx.finish();

    let query = Query::new("abc", &[]).limit(Some(3));
    let results = idx.search(&query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].index, 1);
    assert_eq!(results[0].distance, 1);
}

#[test]
fn it_behaves_with_too_long_inputs() {
    let mut idx = super::Indexer::new();

    /* Single token, multiple duplicated trigrams */
    let long_string = "abc".repeat(1000);
    idx.add_phrase(&long_string, 1, None).unwrap();
    let idx = idx.finish();

    /* Generates 3 different trigrams */
    assert_eq!(3, idx.index.db.len());
    assert!(idx.index.db.contains_key("abc"));
    assert!(idx.index.db.contains_key("bca"));
    assert!(idx.index.db.contains_key("cab"));

    println!("Added {}", long_string);
    let query = Query::new(&long_string, &[]).limit(Some(3));
    let results = idx.search(&query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].index, 1);

    /* A lot of small tokens */
    let mut idx = super::Indexer::new();
    let long_string = "abc ".repeat(70000);
    idx.add_phrase(&long_string, 1, None).unwrap();
    let idx = idx.finish();

    /* Generates only one trigram */
    assert_eq!(1, idx.index.db.len());
    assert!(idx.index.db.contains_key("abc"));

    let query = Query::new("abc", &[]).limit(Some(3));
    let results = idx.search(&query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].index, 1);
    assert_eq!(results[0].distance, 0);
}

#[test]
fn it_detects_duplicate_phrase_idx() {
    let mut idx = super::Indexer::new();

    assert!(idx.add_phrase("phrase one, rather long", 1, None).is_ok());
    assert!(idx.add_phrase("phrase two, duplicated id", 1, None).is_err());
    assert!(idx.add_phrase("phrase three", 1, None).is_err());
    let idx = idx.finish();

    let query = Query::new("rather", &[]).limit(Some(3));
    let results = idx.search(&query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].index, 1);

    let query = Query::new("duplicated", &[]).limit(Some(3));
    let results = idx.search(&query);
    assert_eq!(results.len(), 0);
}
