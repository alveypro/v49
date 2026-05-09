from sklearn.ensemble import RandomForestClassifier

from src.utils.runtime_env import with_model_threads

from .base_model import BaseModel


class RandomForestModel(BaseModel):
    def __init__(self, params):
        super().__init__()
        self.model = RandomForestClassifier(**with_model_threads(params, 'n_jobs'))
