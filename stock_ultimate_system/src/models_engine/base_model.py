import joblib
from sklearn.metrics import accuracy_score, balanced_accuracy_score, brier_score_loss

from src.utils.runtime_env import ensure_parent_dir


class BaseModel:
    def __init__(self):
        self.model = None

    def train(self, X_train, y_train):
        self.model.fit(X_train, y_train)
        return self

    def predict(self, X):
        return self.model.predict(X)

    def predict_proba(self, X):
        return self.model.predict_proba(X) if hasattr(self.model, 'predict_proba') else self.predict(X)

    def save(self, path):
        joblib.dump(self.model, ensure_parent_dir(path))

    def evaluate(self, X_test, y_test):
        pred = self.predict(X_test)
        result = {
            'accuracy': float(accuracy_score(y_test, pred)),
            'balanced_accuracy': float(balanced_accuracy_score(y_test, pred)),
        }
        try:
            proba = self.predict_proba(X_test)
            if hasattr(proba, '__len__'):
                if hasattr(proba[0], '__len__'):
                    positive = [float(row[1]) for row in proba]
                else:
                    positive = [float(v) for v in proba]
                result['brier_score'] = float(brier_score_loss(y_test, positive))
        except Exception:
            pass
        return result
