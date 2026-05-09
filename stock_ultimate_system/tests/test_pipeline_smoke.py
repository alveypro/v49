from pathlib import Path

import yaml
import pytest

from src.pipeline.pipeline_manager import PipelineManager


def _write_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        yaml.safe_dump(content, f, allow_unicode=False, sort_keys=False)


def test_pipeline_smoke_train_predict_backtest(tmp_path):
    config_dir = tmp_path / 'config'
    _write_yaml(config_dir / 'settings.yaml', {
        'project': {'name': 'smoke', 'market': 'CN_A', 'version': 'test'},
        'data': {
            'provider': 'local_stub',
            'fallback_provider': 'local_stub',
            'start_date': '2025-01-01',
            'end_date': '2025-08-31',
            'stock_pool': ['000001.SZ'],
        },
        'training': {'train_ratio': 0.7, 'valid_ratio': 0.15, 'enable_deep_models': False},
        'risk': {'stop_loss_pct': 0.05, 'take_profit_pct': 0.10, 'max_position_pct': 0.2, 'max_drawdown_protection': 0.1},
        'backtest': {
            'initial_cash': 1_000_000,
            'commission_rate': 0.0003,
            'slippage_rate': 0.0005,
            'stamp_tax_rate': 0.001,
            't_plus_one': True,
            'price_limit_check': True,
        },
    })
    _write_yaml(config_dir / 'model_params.yaml', {
        'logistic': {'max_iter': 200},
        'random_forest': {'n_estimators': 20, 'max_depth': 4},
        'lightgbm': {'objective': 'binary', 'n_estimators': 20, 'learning_rate': 0.05, 'num_leaves': 15},
        'xgboost': {'objective': 'binary:logistic', 'n_estimators': 20, 'learning_rate': 0.05},
    })
    _write_yaml(config_dir / 'feature_params.yaml', {
        'ma_windows': [3, 5, 10],
        'ema_windows': [3, 5],
        'vol_windows': [5, 10],
    })
    _write_yaml(config_dir / 'signal_rules.yaml', {
        'strong_buy_score': 50,
        'buy_score': 38,
        'watch_score': 28,
        'sell_score': 20,
    })
    _write_yaml(config_dir / 'risk_rules.yaml', {
        'max_drawdown_protection': 0.1,
        'volatility_filter_threshold': 0.3,
        'liquidity_min_turnover': 1_000_000,
        'range_confidence_min': 0.0,
    })
    _write_yaml(config_dir / 'market_rules.yaml', {
        'main_board_limit': 0.10,
        'st_limit': 0.05,
        'chinext_limit': 0.20,
        'star_limit': 0.20,
        't_plus_one': True,
        'filter_st': True,
        'filter_new_stock_days': 60,
        'liquidity_min_turnover': 1_000_000,
    })

    pm = PipelineManager(str(config_dir))

    train_result = pm.run_training_pipeline('000001.SZ')
    assert train_result.get('trained_models')
    assert Path(train_result['experiment_path']).exists()

    pred_result = pm.run_prediction_pipeline('000001.SZ')
    assert pred_result.get('signal_result') is not None
    assert 'forecast_result' in pred_result
    assert 'style_snapshot' in pred_result
    assert 'hist_vol_20' in pred_result['style_snapshot']

    batch_result = pm.run_batch_prediction(['000001.SZ'])
    assert batch_result.get('results')
    assert batch_result.get('skipped') == []

    bt_result = pm.run_backtest_pipeline(['000001.SZ'])
    assert bt_result.get('status') == 'ok'
    assert Path(bt_result['experiment_path']).exists()

    lean_result = pm.run_backtest_pipeline(['000001.SZ'], generate_artifacts=False)
    assert lean_result.get('status') == 'ok'
    assert Path(lean_result['experiment_path']).exists()
    assert lean_result.get('report_path', '') == ''


def test_run_prediction_pipeline_rejects_empty_training_frame(tmp_path):
    pm = object.__new__(PipelineManager)

    class StubData:
        @staticmethod
        def prepare_dataset(ts_code):
            return {'ts_code': ts_code}

    class StubFeature:
        @staticmethod
        def build_features(df):
            return df

        @staticmethod
        def prepare_training_frame(df):
            import pandas as pd
            return pd.DataFrame(), [], 'label_direction_5'

    pm.data_agent = StubData()
    pm.feature_agent = StubFeature()
    pm.forecast_agent = None
    pm.regime_agent = None
    pm.risk_agent = None
    pm.signal_agent = None
    pm.position_agent = None

    with pytest.raises(ValueError, match='Insufficient feature history'):
        pm.run_prediction_pipeline('688818.SH')


def test_run_batch_prediction_tracks_skipped_symbols():
    pm = object.__new__(PipelineManager)
    pm.reporter = type('Reporter', (), {'generate_signal_report': staticmethod(lambda results: None)})()
    pm._build_pooled_training_frame = lambda ts_codes: None
    pm.run_prediction_pipeline = lambda code: {'ts_code': code} if code == '000001.SZ' else (_ for _ in ()).throw(ValueError(f'Insufficient feature history for {code}'))

    batch_result = pm.run_batch_prediction(['000001.SZ', '688818.SH'])
    assert [item['ts_code'] for item in batch_result['results']] == ['000001.SZ']
    assert batch_result['skipped'] == [{'ts_code': '688818.SH', 'reason': 'Insufficient feature history for 688818.SH'}]
