#!/usr/bin/env python3
import json
import subprocess
import sys

# Read the subscription JSON
with open('/app/subscription.json', 'r') as f:
    subscription_data = json.load(f)

# Use RocksDB Python bindings or a simple approach
# Since we don't have Python rocksdb, let's create a file that the app can read
key = f"subscription:{subscription_data['to']}"
value = json.dumps(subscription_data, separators=(',', ':'))

print(f"Key: {key}")
print(f"Value: {value}")

# Write to a file that we can use with RocksDB ldb tool
with open('/app/sub_key.txt', 'w') as f:
    f.write(key)
with open('/app/sub_value.txt', 'w') as f:
    f.write(value)

print("Files created successfully")
