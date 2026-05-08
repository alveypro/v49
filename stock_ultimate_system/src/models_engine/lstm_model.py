from __future__ import annotations

import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class LSTMModel:
    """LSTM model for sequence-based stock prediction using PyTorch."""

    def __init__(self, params: dict | None = None) -> None:
        params = params or {}
        self.seq_len = params.get('seq_len', 20)
        self.hidden_size = params.get('hidden_size', 64)
        self.num_layers = params.get('num_layers', 2)
        self.dropout = params.get('dropout', 0.2)
        self.epochs = params.get('epochs', 50)
        self.lr = params.get('lr', 0.001)
        self.batch_size = params.get('batch_size', 32)
        self.model = None
        self._input_size: int = 0
        self._device = 'cpu'
        self._fitted = False

    def _build_model(self, input_size: int) -> Any:
        import torch
        import torch.nn as nn

        class _LSTMNet(nn.Module):
            def __init__(self, in_size, hidden, n_layers, drop):
                super().__init__()
                self.lstm = nn.LSTM(in_size, hidden, n_layers, batch_first=True, dropout=drop if n_layers > 1 else 0)
                self.fc = nn.Sequential(
                    nn.Linear(hidden, 32),
                    nn.ReLU(),
                    nn.Dropout(drop),
                    nn.Linear(32, 2),
                )

            def forward(self, x):
                out, _ = self.lstm(x)
                return self.fc(out[:, -1, :])

        self._input_size = input_size
        return _LSTMNet(input_size, self.hidden_size, self.num_layers, self.dropout)

    def _make_sequences(self, X: np.ndarray, y: np.ndarray | None = None):
        import torch
        seqs, labels = [], []
        for i in range(len(X) - self.seq_len):
            seqs.append(X[i:i + self.seq_len])
            if y is not None:
                labels.append(y[i + self.seq_len])
        X_t = torch.FloatTensor(np.array(seqs))
        y_t = torch.LongTensor(np.array(labels)) if y is not None else None
        return X_t, y_t

    def train(self, X_train, y_train):
        import torch
        import torch.nn as nn
        from torch.utils.data import TensorDataset, DataLoader

        X = np.array(X_train, dtype=np.float32)
        y = np.array(y_train, dtype=np.int64)

        X_seq, y_seq = self._make_sequences(X, y)
        if len(X_seq) == 0:
            logger.warning('Not enough data for LSTM (need > %d rows)', self.seq_len)
            return self

        self.model = self._build_model(X.shape[1]).to(self._device)
        dataset = TensorDataset(X_seq, y_seq)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)

        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr)
        criterion = nn.CrossEntropyLoss()

        self.model.train()
        for epoch in range(self.epochs):
            total_loss = 0
            for xb, yb in loader:
                xb, yb = xb.to(self._device), yb.to(self._device)
                optimizer.zero_grad()
                logits = self.model(xb)
                loss = criterion(logits, yb)
                loss.backward()
                optimizer.step()
                total_loss += loss.item()
            if (epoch + 1) % 10 == 0:
                logger.debug('LSTM epoch %d/%d loss=%.4f', epoch + 1, self.epochs, total_loss / len(loader))

        self._fitted = True
        return self

    def predict(self, X):
        if not self._fitted or self.model is None:
            return np.zeros(len(X))
        import torch
        X = np.array(X, dtype=np.float32)
        X_seq, _ = self._make_sequences(X)
        if len(X_seq) == 0:
            return np.zeros(len(X))
        self.model.eval()
        with torch.no_grad():
            logits = self.model(X_seq.to(self._device))
            preds = logits.argmax(dim=1).cpu().numpy()
        result = np.zeros(len(X))
        result[self.seq_len:self.seq_len + len(preds)] = preds
        return result

    def predict_proba(self, X):
        if not self._fitted or self.model is None:
            n = len(X) if hasattr(X, '__len__') else 1
            return np.full((n, 2), 0.5)
        import torch
        X = np.array(X, dtype=np.float32)
        X_seq, _ = self._make_sequences(X)
        if len(X_seq) == 0:
            return np.full((len(X), 2), 0.5)
        self.model.eval()
        with torch.no_grad():
            logits = self.model(X_seq.to(self._device))
            proba = torch.softmax(logits, dim=1).cpu().numpy()
        result = np.full((len(X), 2), 0.5)
        result[self.seq_len:self.seq_len + len(proba)] = proba
        return result

    def save(self, path: str) -> None:
        if self.model is None:
            return
        import torch
        torch.save(self.model.state_dict(), path)

    def load(self, path: str, input_size: int) -> None:
        import torch
        self.model = self._build_model(input_size)
        self.model.load_state_dict(torch.load(path, map_location=self._device))
        self._fitted = True
