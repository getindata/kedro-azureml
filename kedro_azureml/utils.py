from copy import deepcopy
from dataclasses import dataclass
from typing import Any


@dataclass
class CliContext:
    env: str
    metadata: Any


def update_dict(dictionary, *kv_pairs):
    """Return a deep copy of dictionary with updated values for the given key-value pairs.
    Supports nested dictionaries"""
    updated = deepcopy(dictionary)

    def traverse(d, key, value):
        s = key.split(".", 1)
        if len(s) > 1:
            if (s[0] not in d) or (not isinstance(d[s[0]], dict)):
                d[s[0]] = {}
            traverse(d[s[0]], s[1], value)
        else:
            d[s[0]] = value

    for k, v in kv_pairs:
        traverse(updated, k, v)
    return updated
