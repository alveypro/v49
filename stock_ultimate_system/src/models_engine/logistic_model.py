from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .base_model import BaseModel


class LogisticModel(BaseModel):
    def __init__(self, params):
        super().__init__()
        self.model = make_pipeline(
            StandardScaler(),
            LogisticRegression(**params),
        )
