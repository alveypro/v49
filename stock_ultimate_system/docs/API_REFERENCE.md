# API 参考

## PipelineManager
- `run_system_demo()`
- `run_training_pipeline()`
- `run_prediction_pipeline(ts_code)`
- `run_backtest_pipeline(stock_pool)`

## DataAgent
- `prepare_dataset(ts_code)`
- `load_market_dataset(ts_code)`

## FeatureAgent
- `build_features(df)`
- `prepare_training_frame(df)`

## ForecastAgent
- `train_models(df, feature_cols, target_col)`
- `predict(df, feature_cols)`

## SignalAgent
- `generate_signal(...)`

## RiskAgent
- `evaluate_trade_risk(...)`

## PositionAgent
- `calculate_position_size(...)`
