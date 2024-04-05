import random
import uuid
import time
import json

record = {
    "record_id": str(uuid.uuid4()),
    "timestamp": time.time(),
    "message": "test record",
}

print(json.dumps(record))
