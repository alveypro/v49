import json
from pathlib import Path

import joblib

from src.utils.runtime_env import ensure_parent_dir


def save_json(data: dict, path: str) -> None:
    target = ensure_parent_dir(path)
    with target.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(path: str) -> dict:
    with Path(path).open('r', encoding='utf-8') as f:
        return json.load(f)


def save_joblib(obj, path: str) -> None:
    target = ensure_parent_dir(path)
    joblib.dump(obj, target)
