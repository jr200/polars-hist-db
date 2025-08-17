from typing import Any, Iterable, Mapping
import os
import yaml
from functools import reduce


def get_nested_key(my_dict: Mapping[str, Any], keys: Iterable[str]):
    try:
        return reduce(lambda d, key: d[key], keys, my_dict)
    except (KeyError, TypeError):
        return None


def load_yaml(filename: str) -> Any:
    def _expand_env_vars(value: Any) -> Any:
        if isinstance(value, str):
            return os.path.expandvars(value)
        elif isinstance(value, dict):
            return {k: _expand_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [_expand_env_vars(v) for v in value]
        else:
            return value

    print(f"loading Config from: {filename}")
    with open(filename, "r") as file:
        yaml_dict = yaml.safe_load(file)
        yaml_dict = _expand_env_vars(yaml_dict)

    return yaml_dict
