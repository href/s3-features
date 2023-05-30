import secrets


def random_bucket_name() -> str:
    return f's3f-{secrets.token_hex(8)}'
