import random
import uuid
import time
import json
import sys

n = int(sys.argv[1])


MAX_SIZE = 1024 * 1024

random.seed(42)

words = ["alpha", "bravo", "charlie", "delta", "echo"]

def make_int():
    return random.randint(0, 100)

def make_float():
    return random.random() * 100

def make_bool():
    return random.random() < 0.5

def make_string():
    n = random.randint(1, 10)
    return " ".join(random.choice(words) for _ in range(n))

def make_value():
    options = [make_int, make_float, make_string, make_bool]
    if random.random() < 0.2:
        options.append(make_array)
        options.append(make_object)
    return random.choice(options)()

def make_array():
    n = random.randint(1, 5)
    return [make_value() for _ in range(n)]

def make_object():
    n = random.randint(1, 5)
    keys = random.sample(words, n)
    return {key: make_value() for key in keys}

def make_record():
    return {
        "record_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        **make_object(),
    }

for _ in range(n):
    while True:
        record = make_record()
        encoded = json.dumps(record)
        if len(encoded) > MAX_SIZE:
            continue
        break
    print(encoded)