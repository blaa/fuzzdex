# FuzzDex

Fuzzy Index for Python, written in Rust. Works like an error-tolerant dict,
keyed by a human input.

## Usecases

I use it for matching parts of user supplied physical addresses to data
extracted from OSM map to find streets and cities. Original solution used
Elasticsearch database with a fuzzy query, which worked - but was 21x slower.
