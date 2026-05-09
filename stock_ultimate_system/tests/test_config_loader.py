from pathlib import Path

import yaml

from src.config.config_loader import ConfigLoader


def _write_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        yaml.safe_dump(content, f, allow_unicode=False, sort_keys=False)


def test_load_yaml_caches_file_contents(tmp_path):
    config_dir = tmp_path / 'config'
    _write_yaml(config_dir / 'settings.yaml', {'project': {'name': 'demo'}})

    loader = ConfigLoader(str(config_dir))
    first = loader.load_yaml('settings.yaml')
    _write_yaml(config_dir / 'settings.yaml', {'project': {'name': 'changed'}})

    second = loader.load_yaml('settings.yaml')
    assert first == second
    assert second['project']['name'] == 'demo'


def test_load_all_configs_supports_yml_and_yaml(tmp_path):
    config_dir = tmp_path / 'config'
    _write_yaml(config_dir / 'settings.yaml', {'a': 1})
    _write_yaml(config_dir / 'market_rules.yml', {'b': 2})

    loader = ConfigLoader(str(config_dir))
    loaded = loader.load_all_configs()

    assert loaded['settings']['a'] == 1
    assert loaded['market_rules']['b'] == 2
