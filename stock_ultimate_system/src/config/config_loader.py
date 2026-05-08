from pathlib import Path
from typing import Any
import yaml

from src.utils.project_paths import resolve_project_path


class ConfigLoader:
    def __init__(self, config_dir: str) -> None:
        self.config_dir = resolve_project_path(config_dir)
        self._cache: dict[str, dict[str, Any]] = {}

    def load_yaml(self, file_name: str) -> dict[str, Any]:
        path = self.config_dir / file_name
        cache_key = path.stem
        if cache_key in self._cache:
            return self._cache[cache_key]
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')
        with path.open('r', encoding='utf-8') as f:
            content = yaml.safe_load(f) or {}
        self._cache[cache_key] = content
        return content

    def load_all_configs(self) -> dict[str, dict[str, Any]]:
        self._cache = {}
        paths = sorted(self.config_dir.glob('*.yaml')) + sorted(self.config_dir.glob('*.yml'))
        for path in paths:
            self.load_yaml(path.name)
        return self._cache

    def get(self, section: str, key: str | None = None, default: Any = None) -> Any:
        if not self._cache:
            self.load_all_configs()
        sec = self._cache.get(section, {})
        return sec if key is None else sec.get(key, default)
