import os

from src.utils.runtime_env import configure_runtime_environment, default_model_threads, with_model_threads


def test_configure_runtime_environment_sets_loky_limit(monkeypatch):
    monkeypatch.delenv('LOKY_MAX_CPU_COUNT', raising=False)
    configure_runtime_environment()
    assert int(os.environ['LOKY_MAX_CPU_COUNT']) >= 1


def test_with_model_threads_preserves_explicit_values(monkeypatch):
    monkeypatch.setenv('STOCK_SYSTEM_MODEL_THREADS', '3')
    params = with_model_threads({'n_jobs': 7}, 'n_jobs')
    assert params['n_jobs'] == 7
    assert default_model_threads() == 3


def test_with_model_threads_adds_defaults(monkeypatch):
    monkeypatch.setenv('STOCK_SYSTEM_MODEL_THREADS', '2')
    params = with_model_threads({}, 'n_jobs', 'nthread')
    assert params['n_jobs'] == 2
    assert params['nthread'] == 2
