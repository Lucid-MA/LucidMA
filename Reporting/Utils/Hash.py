import hashlib


def hash_string(string):
    hash_object = hashlib.sha256(string.encode("utf-8"))  # SHA-256 hash
    hex_digest = hash_object.hexdigest()
    decimal_value = int(hex_digest[:12], 16)
    return decimal_value  # Convert first 12 hex digits


def hash_string_v2(string) -> str:
    hash_object = hashlib.sha256(string.encode("utf-8"))  # SHA-256 hash
    hex_digest = hash_object.hexdigest()
    decimal_value = int(hex_digest[:12], 16)  # Convert first 12 hex digits to decimal
    truncated_value = decimal_value % 10**12  # Truncate to 12 digits
    string_value = str(truncated_value).zfill(
        12
    )  # Convert to string and pad with leading zeros
    return string_value
