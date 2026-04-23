from __future__ import annotations

from dataclasses import asdict, fields, is_dataclass
from enum import Enum
from typing import Any, get_args, get_origin, get_type_hints


def to_primitive(value: Any) -> Any:
    if is_dataclass(value):
        return {field.name: to_primitive(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        return [to_primitive(item) for item in value]
    if isinstance(value, dict):
        return {key: to_primitive(item) for key, item in value.items()}
    return value


def from_primitive(type_hint: Any, value: Any) -> Any:
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin is list:
        return [from_primitive(args[0], item) for item in value]

    if origin is dict:
        key_type, item_type = args
        return {from_primitive(key_type, key): from_primitive(item_type, item) for key, item in value.items()}

    if origin is None and isinstance(type_hint, type) and issubclass(type_hint, Enum):
        return type_hint(value)

    if origin is None and is_dataclass(type_hint):
        hints = get_type_hints(type_hint)
        return type_hint(
            **{
                field.name: from_primitive(hints[field.name], value[field.name])
                for field in fields(type_hint)
            }
        )

    if origin is not None and type(None) in args:
        inner = next(arg for arg in args if arg is not type(None))
        return None if value is None else from_primitive(inner, value)

    return value
