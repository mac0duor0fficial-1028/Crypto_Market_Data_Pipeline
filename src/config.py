"""Central access to environment configuration (all secrets come from .env)."""
import os


def env(name: str, default=None, required: bool = False):
    value = os.environ.get(name, default)
    if required and value in (None, ''):
        raise RuntimeError(f'Missing required environment variable: {name}')
    return value


def kraken_pairs() -> list:
    raw = env('KRAKEN_PAIRS', 'XBTUSD')
    return [p.strip() for p in raw.split(',') if p.strip()]
