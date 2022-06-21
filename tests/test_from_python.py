"""
Test fuzzdex interface from the Python side.

Execution:
$ pipenv install
$ pipenv shell
$ maturin develop
$ pytest
"""

from concurrent.futures import ThreadPoolExecutor
import fuzzdex


def test_basic():
    """Test basic behaviour."""
    fud = fuzzdex.FuzzDex()
    fud.add_phrase("This is an entry.", 1, constraints=set())
    fud.add_phrase("Another entered-entry.", 2, constraints={1})
    fud.add_phrase('Another about "Guacamole".', 3, constraints={1, 2})
    fud.finish()
    results = fud.search("this", [])
    assert len(results) == 1
    assert results[0]['index'] == 1

    results = fud.search("this", [], constraint=1)
    assert len(results) == 0

    results = fud.search("another", [], constraint=1)
    assert len(results) == 2
    print(results)

    results = fud.search("guacamole", [])
    assert len(results) == 1


def test_parallel():
    """Test parallel locking."""
    phrases = []
    fud = fuzzdex.FuzzDex()
    for i in range(100, 300):
        phrase = f"phrase number {i}"
        phrases.append(phrase)
        fud.add_phrase(phrase, i, constraints=set())
    fud.finish()

    executor = ThreadPoolExecutor(max_workers=8)

    def query(phrase):
        tokens = phrase.split()
        return fud.search(tokens[0], [tokens[-1]], limit=1)

    results = executor.map(query, phrases)
    results = list(results)

    indices = [r[0]['index'] for r in results]
    assert len(indices) == len(set(indices))


def test_distance():
    """Test helper distance method."""
    assert fuzzdex.distance("oneword", "oneword") == 0
    assert fuzzdex.distance("oneword", "oneWord") == 1
    assert fuzzdex.distance("oneword", "oneXord") == 1
    assert fuzzdex.distance("oneword", "oneord") == 1
    assert fuzzdex.distance("oneword", "onewXord") == 1
    assert fuzzdex.distance("onword", "onewoXrd") == 2
    assert fuzzdex.distance("żółw", "zolw") == 3
    assert fuzzdex.distance("żółw", "żólw") == 1

    # This is a 2 unicode-character grapheme
    assert fuzzdex.distance("y̆es", "yes") == 1
