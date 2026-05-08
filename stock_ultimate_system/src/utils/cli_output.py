from __future__ import annotations

import json
from typing import Any


def print_header(title: str) -> None:
    print(f'\n=== {title} ===')


def print_kv(label: str, value: Any) -> None:
    print(f'{label}: {value}')


def print_mapping(title: str, mapping: dict[str, Any], float_precision: int = 4, skip_dict_values: bool = False) -> None:
    if not mapping:
        return
    print(f'\n{title}:')
    for key, value in mapping.items():
        if skip_dict_values and isinstance(value, dict):
            continue
        if isinstance(value, float):
            print(f'  {key}: {value:.{float_precision}f}')
        else:
            print(f'  {key}: {value}')


def print_json_kv(label: str, payload: dict[str, Any]) -> None:
    print(f'{label}: {json.dumps(payload, ensure_ascii=False)}')
