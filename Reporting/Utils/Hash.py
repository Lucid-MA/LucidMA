import hashlib


def hash_string(string):
    hash_object = hashlib.sha256(string.encode("utf-8"))  # SHA-256 hash
    hex_digest = hash_object.hexdigest()
    decimal_value = int(hex_digest[:12], 16)
    return decimal_value  # Convert first 12 hex digits
