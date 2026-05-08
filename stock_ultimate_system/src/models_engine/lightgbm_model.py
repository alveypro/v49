from lightgbm import LGBMClassifier

from src.utils.runtime_env import with_model_threads

from .base_model import BaseModel


class LightGBMModel(BaseModel):
    def __init__(self, params):
        super().__init__()
        effective = dict(params or {})
        effective.setdefault('verbosity', -1)
        effective.setdefault('verbose', -1)
        self.model = LGBMClassifier(**with_model_threads(effective, 'n_jobs'))
