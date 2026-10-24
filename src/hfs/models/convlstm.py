"""ConvLSTM encoder-forecaster for gridded t2m and log-precipitation (FR-ML-05).

Implemented in-house (no external ConvLSTM package), per SRS 4.3.
"""
from __future__ import annotations

import torch
from torch import nn


class ConvLSTMCell(nn.Module):
    def __init__(self, in_ch: int, hid_ch: int, kernel: int = 3):
        super().__init__()
        self.hid = hid_ch
        self.conv = nn.Conv2d(in_ch + hid_ch, 4 * hid_ch, kernel, padding=kernel // 2)

    def forward(self, x, state):
        h, c = state
        i, f, o, g = torch.chunk(self.conv(torch.cat([x, h], 1)), 4, dim=1)
        c = torch.sigmoid(f) * c + torch.sigmoid(i) * torch.tanh(g)
        h = torch.sigmoid(o) * torch.tanh(c)
        return h, c

    def init(self, b, hgt, wid, device):
        z = torch.zeros(b, self.hid, hgt, wid, device=device)
        return z, z.clone()


class ConvLSTMForecaster(nn.Module):
    """Encoder (stacked ConvLSTM over L input frames) -> forecaster (same stack unrolled H steps)
    with a skip connection from the last input frame; output (B, H, C_out, Y, X)."""

    def __init__(self, in_ch: int, out_ch: int, hidden=(16, 16), kernel: int = 3, horizon: int = 6):
        super().__init__()
        self.horizon, self.out_ch = horizon, out_ch
        chans = [in_ch] + list(hidden)
        self.enc = nn.ModuleList([ConvLSTMCell(chans[i], chans[i + 1], kernel) for i in range(len(hidden))])
        fchans = [out_ch] + list(hidden)
        self.dec = nn.ModuleList([ConvLSTMCell(fchans[i], fchans[i + 1], kernel) for i in range(len(hidden))])
        self.head = nn.Conv2d(hidden[-1] + out_ch, out_ch, 1)

    def forward(self, x):  # x: (B, L, C, Y, X)
        b, L, _, hy, wx = x.shape
        states = [cell.init(b, hy, wx, x.device) for cell in self.enc]
        for t in range(L):
            inp = x[:, t]
            for k, cell in enumerate(self.enc):
                states[k] = cell(inp, states[k])
                inp = states[k][0]
        last = x[:, -1, :self.out_ch]       # skip: last observed target channels
        outs, prev = [], last
        for _ in range(self.horizon):
            inp = prev
            for k, cell in enumerate(self.dec):
                states[k] = cell(inp, states[k])
                inp = states[k][0]
            y = prev + self.head(torch.cat([inp, prev], 1))   # residual update
            outs.append(y)
            prev = y
        return torch.stack(outs, 1)


def extreme_weighted_mse(pred, target, mask=None, alpha: float = 1.0, q: float = 0.95):
    """Masked MSE + extreme-weighted term: cells above the batch q-quantile of |target| get weight 1+alpha."""
    w = torch.ones_like(target)
    thr = torch.quantile(target.abs().flatten().float(), q)
    w = w + alpha * (target.abs() >= thr).float()
    if mask is not None:
        w = w * mask
    return (w * (pred - target) ** 2).sum() / w.sum().clamp(min=1.0)
