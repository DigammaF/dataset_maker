
from typing import Callable


def get_or_create[K, V](map: dict[K, V], key: K, default_factory: Callable[[], V]) -> V:
    if key not in map:
        map[key] = default_factory()

    return map[key]
