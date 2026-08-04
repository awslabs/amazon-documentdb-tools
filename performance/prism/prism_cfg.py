"""Load prism_config.yaml and expose settings."""
import os
import yaml

_CFG_PATH = os.path.join(os.path.dirname(__file__), "prism_config.yaml")
_LEGACY_CFG = os.path.join(os.path.dirname(__file__), "lazy_load_config.yaml")

# Auto-migrate legacy config filename
if not os.path.exists(_CFG_PATH) and os.path.exists(_LEGACY_CFG):
    os.rename(_LEGACY_CFG, _CFG_PATH)

_cache = None


def get_config():
    global _cache
    if _cache is None:
        with open(_CFG_PATH) as f:
            _cache = yaml.safe_load(f)
    return _cache
