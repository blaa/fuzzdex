# FuzzDex

FuzzDex is a fast Python library, written in Rust. It implements an in-memory
`fuzzy index` that works like an error-tolerant dictionary keyed by a human
input.

## Algorithm

You load into fuzzdex series of short phrases - like street names consisting of
one or multiple words, with an numerical index that identifies this street names
in related dictionary of cities.

Then, you can query the index using a `must-token` (currently only one, but
could be altered to use more) and additional `should-tokens` to read a total of
`limit` of possibly matching phrases.

Must-token is trigramized (warszawa -> war ars rsz sza zaw awa) and all phrases
containing given trigrams are initially read from the index. Trigrams have
scores, the more common they are, the less they increase the phrase score.
Trigrams of should-tokens additionally alter the score (positively when they
match), but don't add additional phrases from index. Phrases are then sorted by
score.

Top phrases are filtered to contain an optional constraint and the must-token
with a maximal editing distance (Levenshtein) until `limit` of phrases is
gathered.

Internally, the results of a must-token search are LRU cached as in practise
it's pretty often repeated. Should-tokens vary and they are always recalculated.

## Usecases

It was designed to match parts of a user supplied physical addresses to a data
extracted from the OpenStreet map - in order to find streets and cities.

Address is first tokenized and then it's parts are matched against fuzzy
dictionary of cities and streets. Additional constraints can limit the matched
streets only to given city - or finding cities that have a given street.

Data is first searched for using trigrams (warszawa -> war ars rsz sza zaw awa),
and then additionally filtered using maximal Levenshtein editing distance.

Original solution used fuzzy query of the Elasticsearch database, which worked -
but was 21x slower in our tests.

## Example

```python
import fuzzdex
# Create two fuzzy indices with cities and streets.
cities = fuzzdex.FuzzDex()
# Warsaw has streets: Czerniakowska, Nowy Świat and Wawelska
cities.add_phrase("Warsaw", 1, constraints={1, 2, 3})
# Wrocław only Czerniawska
cities.add_phrase("Wrocław", 2, constraints={4})

streets = fuzzdex.FuzzDex()
# Streets with reversed constraints and own indices:
streets.add_phrase("Czerniakowska", 1, constraints={1})
streets.add_phrase("Nowy Świat", 2, constraints={1})
streets.add_phrase("Wawelska", 3, constraints={1})

streets.add_phrase("Czerniawska", 4, constraints={2})

# This recalculates trigram scores and makes index immutable:
cities.finish()
streets.finish()

# warszawa matches warsaw at editing distance 2.
cities.search("warszawa", [], max_distance=2, limit=60)
#    [{'origin': 'Warsaw', 'index': 1, 'token': 'warsaw',
#      'distance': 2, 'score': 200000.0, 'should_score': 0.0}]

# `świat` adds additional should score to the result and places it higher
# in case the limit is set:
streets.search("nowy", ["świat"], max_distance=2, constraint=1)
#    [{'origin': 'Nowy Świat', 'index': 2, 'token': 'nowy',
#      'distance': 0, 'score': 5.999, 'should_score': 7.4999}]

# Won't match with constraint 2.
streets.search("nowy", ["świat"], constraint=2)
#    []

# Quering for `czerniawska` will return `czerniakowska` (no constraints),
# but with a lower score and higher distance:
In [22]: streets.search("czerniawska", [], max_distance=2)
Out[22]:
#  [{'origin': 'Czerniawska', 'index': 4, 'token': 'czerniawska',
#   'distance': 0, 'score': 9.49995231628418, 'should_score': 0.0},
#  {'origin': 'Czerniakowska', 'index': 1, 'token': 'czerniakowska',
#   'distance': 2, 'score': 6.4999680519104, 'should_score': 0.0}]
```

## Installation, development

You can install fuzzdex from PyPI when using one of the architectures it's
published for (x86_64, few Python versions).

    pip3 install fuzzdex

Or use `maturin` to build it locally:

    pipenv install --dev
    pipenv shell
    maturin develop -r
    pytest

You can also use cargo and copy or link the .so file directly (rename
libfuzzdex.so to fuzzdex.so):

    cargo build --release
    ln -s target/release/libfuzzdex.so fuzzdex.so

`build.sh` has commands for building manylinux packages for PyPI.
