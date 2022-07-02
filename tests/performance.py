#!/usr/bin/env python3

"""
Called manually, but requires OSM data exported with
https://github.com/exatel/topo_import in .csv.gz file
"""

import gzip
import csv
from time import time
import random
import fuzzdex
import pickle
from dataclasses import dataclass, field
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool


@dataclass
class Address:
    housenumber: str
    lon: str
    lat: str
    postcode: str

@dataclass
class Entry:
    eid: int
    name: str
    constraints: set = field(default_factory=set)


def read_csv():
    """Read CSV, generate Pickle."""
    tree = defaultdict(lambda: {})

    cities = {}
    streets = {}

    city_cnt = 0
    street_cnt = 1  # 0 == No street

    test_data = []

    with gzip.open("osm-export.csv.gz", "rt") as csvgz:
        reader = csv.reader(csvgz)
        _ = next(reader)

        # pid,name,city,postcode,street,housenumber,simc,amenity,lon,lat,street_distance,city_from_area
        for i, row in enumerate(reader):
            city_name, postcode, street_name, housenumber = row[2:6]
            lon, lat = row[8], row[9]

            address = Address(housenumber=housenumber,
                              lon=lon, lat=lat, postcode=postcode)

            # Get/create city and street:
            city = cities.get(city_name)
            if city is None:
                # New city
                city = Entry(eid=city_cnt, name=city_name)
                cities[city_name] = city
                city_cnt += 1

            if street_name:
                # Streets are sometimes not there.
                street = streets.get(street_name)
                if street is None:
                    street = Entry(eid=street_cnt, name=street_name)
                    streets[street_name] = street
                    street_cnt += 1
                street_id = street.eid

                # Bind street and city
                city.constraints.add(street.eid)
                street.constraints.add(city.eid)
            else:
                street_id = 0

            if i % 2000 == 0:
                test_data.append((city_name, street_name, housenumber))

            tree[(city.eid, street_id)][address.housenumber] = address

            if (i+1) % 100000 == 0:
                print(f"Loaded {i+1} rows, {len(cities)} cities, {len(streets)} streets.")

    start = time()
    tree = dict(tree)
    random.shuffle(test_data)
    with gzip.open("pickle-dump.pickle.gz", "wb") as gz:
        pickle.dump((cities, streets, tree, test_data), gz)
    print(f"Pickle dump took {time() - start}")
    return


def load():
    """Load pickle and index data."""
    # city -> street -> housenumber -> Address
    # (city, street) -> housenumber -> Address?
    start = time()
    with gzip.open("pickle-dump.pickle.gz", "rb") as gz:
        cities, streets, tree, test_data = pickle.load(gz)
    print(f"Unpickle took {time() - start}")

    print("Building fuzzdex index")
    start = time()
    # Pump data into FuzzDex since it's complete and constraints won't change
    city_idx = fuzzdex.FuzzDex()

    for city in cities.values():
        city_idx.add_phrase(city.name, city.eid, city.constraints)
    city_idx.finish()

    street_idx = fuzzdex.FuzzDex()
    for street in streets.values():
        street_idx.add_phrase(street.name, street.eid, street.constraints)
    street_idx.finish()
    print(f"Build took {time() - start}")

    return (city_idx, street_idx, tree, test_data)


# read_csv()
start = time()
city_idx, street_idx, tree, test_data = load()

took = time() - start
print(f"Data loaded in {took}")


def prepare(phrase):
    tokens = fuzzdex.tokenize(phrase)
    tokens.sort(key=len, reverse=True)
    if not tokens:
        return phrase, []
    return tokens[0], tokens[1:]


def scan_streets(street, city_id, housenumber, limit):
    must, should = prepare(street)
    streets = street_idx.search(must, should, constraint=city_id,
                                max_distance=2, limit=limit)
    if not streets:
        return False

    for street_solution in streets:
        street_id = street_solution["index"]
        data = tree.get((city_id, street_id), {}).get(housenumber)
        if data:
            return True
    return False


def scan_city(city, street, housenumber, limit=20):
    must, should = prepare(city)
    cities = city_idx.search(must, should, max_distance=2, limit=limit)
    for city_solution in cities:
        city_id = city_solution["index"]
        if not street:
            street_id = 0
            data = tree.get((city_id, street_id), {}).get(housenumber)
            if data is not None:
                return True
            continue

        if scan_streets(street, city_id, housenumber, limit):
            return True
    return False


def test_geo(limit=20):
    s = time()
    found = 0
    not_found = 0
    for i, (city, street, housenumber) in enumerate(test_data):
        got = scan_city(city, street, housenumber, limit=limit)
        if got:
            found += 1
        else:
            not_found += 1

        if i % 100 == 0:
            took = time() - s
            print(f"DID {i} in {took} {i / took}/s")

    took = time() - s
    print(f"DID {len(test_data)} in {took}")
    print(f"found={found} not_found={not_found}")


config = {'limit': 30}

def do(entry):
    "For parallel mapping"
    city, street, housenumber = entry
    if scan_city(city, street, housenumber, limit=config['limit']):
        return 1
    else:
        return 0


def test_parallel(workers=8, limit=20):
    executor = ThreadPoolExecutor(max_workers=workers)
    config['limit'] = limit

    s = time()
    results = executor.map(do, test_data)
    results = list(results)
    found = sum(results)
    took = time() - s

    cnt = len(test_data)
    print(f"DID {cnt} on {workers} threads in {took:.3f}, "
          f"{cnt / took:.2f}/s ({cnt/took/workers:.1f} per thread), "
          f"{took / cnt * 1000:.4f}ms/q ({found}/{len(results)})")


def test_parallel_mp(workers=8, limit=20, chunk_size=None):
    pool = Pool(processes=workers)
    config['limit'] = limit

    s = time()
    results = pool.map(do, test_data, chunk_size)
    results = list(results)
    found = sum(results)
    took = time() - s

    cnt = len(test_data)
    print(f"DID {cnt} on {workers} processes in {took:.3f}, "
          f"{cnt / took:.2f}/s ({cnt/took/workers:.1f} per process), "
          f"{took / cnt * 1000:.4f}ms/q ({found}/{len(results)})")


print("Pre-heat cache:")
test_parallel(workers=1)
print("OK:")
test_parallel(workers=1)
test_parallel(workers=3)
test_parallel(workers=6)
test_parallel(workers=8)
test_parallel_mp(workers=1)
test_parallel_mp(workers=3)
test_parallel_mp(workers=6)
test_parallel_mp(workers=8)
