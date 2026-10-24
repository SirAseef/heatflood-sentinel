"""LSTM multi-horizon forecaster with city embedding (FR-ML-04), PyTorch, in-house."""
from __future__ import annotations

import numpy as np
import torch
from torch import nn


class LSTMForecaster(nn.Module):
    """Seq2one-vector direct multi-horizon: (B, L, F) + city id -> (B, H)."""

    def __init__(self, n_features: int, n_horizons: int, n_cities: int, hidden: int = 64, layers: int = 2,
                 dropout: float = 0.1, emb_dim: int = 4, bidirectional: bool = False):
        super().__init__()
        self.emb = nn.Embedding(n_cities, emb_dim)
        self.lstm = nn.LSTM(n_features + emb_dim, hidden, num_layers=layers, batch_first=True,
                            dropout=dropout if layers > 1 else 0.0, bidirectional=bidirectional)
        out = hidden * (2 if bidirectional else 1)
        self.head = nn.Sequential(nn.Linear(out, out), nn.ReLU(), nn.Dropout(dropout), nn.Linear(out, n_horizons))

    def forward(self, x: torch.Tensor, city: torch.Tensor) -> torch.Tensor:
        e = self.emb(city).unsqueeze(1).expand(-1, x.shape[1], -1)
        h, _ = self.lstm(torch.cat([x, e], dim=-1))
        return self.head(h[:, -1])


def make_windows(X: np.ndarray, Y: np.ndarray, groups: np.ndarray, lookback: int, idx_ok: np.ndarray):
    """Return indices t (end of window) where a full lookback window inside one group exists and idx_ok[t]."""
    ends = []
    start = 0
    n = len(groups)
    while start < n:
        stop = start
        while stop < n and groups[stop] == groups[start]:
            stop += 1
        cand = np.arange(start + lookback - 1, stop)
        ends.append(cand[idx_ok[cand]])
        start = stop
    return np.concatenate(ends) if ends else np.array([], dtype=int)


class WindowDataset(torch.utils.data.Dataset):
    def __init__(self, X: np.ndarray, Y: np.ndarray, city: np.ndarray, ends: np.ndarray, lookback: int):
        self.X, self.Y, self.city, self.ends, self.L = X, Y, city, ends, lookback

    def __len__(self):
        return len(self.ends)

    def __getitem__(self, i):
        t = self.ends[i]
        return (torch.from_numpy(self.X[t - self.L + 1:t + 1]), torch.tensor(self.city[t]),
                torch.from_numpy(self.Y[t]))


def train_model(model: nn.Module, train_dl, val_dl, epochs: int, lr: float, patience: int = 3,
                weight_fn=None, log=None) -> dict:
    """Adam + early stopping on val loss (patience), grad clipping 1.0, keep best weights."""
    opt = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    best, best_state, bad, hist = np.inf, None, 0, []
    for ep in range(epochs):
        model.train()
        tl = []
        for xb, cb, yb in train_dl:
            opt.zero_grad()
            pred = model(xb, cb)
            w = weight_fn(yb) if weight_fn else 1.0
            loss = (w * (pred - yb) ** 2).mean()
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            tl.append(loss.item())
        model.eval()
        with torch.no_grad():
            vl = float(np.mean([((model(xb, cb) - yb) ** 2).mean().item() for xb, cb, yb in val_dl]))
        hist.append({"epoch": ep + 1, "train_loss": float(np.mean(tl)), "val_loss": vl})
        if log:
            log.info("epoch %d train %.4f val %.4f", ep + 1, np.mean(tl), vl)
        if vl < best - 1e-5:
            best, bad = vl, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= patience:
                break
    if best_state:
        model.load_state_dict(best_state)
    return {"history": hist, "best_val": best}


def predict(model: nn.Module, dl) -> np.ndarray:
    model.eval()
    out = []
    with torch.no_grad():
        for xb, cb, _ in dl:
            out.append(model(xb, cb).numpy())
    return np.concatenate(out) if out else np.zeros((0,))
