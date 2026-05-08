from __future__ import annotations

import json
import pickle
from pathlib import Path

import pandas as pd


class DataStorage:
    """Unified local storage for csv / parquet / pickle / json."""

    def save_csv(self, df: pd.DataFrame, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)

    def load_csv(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    def save_parquet(self, df: pd.DataFrame, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    def load_parquet(self, path: str) -> pd.DataFrame:
        return pd.read_parquet(path)

    def save_pickle(self, obj: object, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(obj, f)

    def load_pickle(self, path: str) -> object:
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save_json(self, data: dict, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def load_json(self, path: str) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
