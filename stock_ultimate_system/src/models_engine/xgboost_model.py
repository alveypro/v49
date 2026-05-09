from xgboost import XGBClassifier

from src.utils.runtime_env import with_model_threads

from .base_model import BaseModel


class XGBoostModel(BaseModel):
    def __init__(self, params):
        super().__init__()
        effective = dict(params or {})
        effective.setdefault('verbosity', 0)
        effective.setdefault('use_label_encoder', False)
        self.model = XGBClassifier(**with_model_threads(effective, 'n_jobs', 'nthread'))
