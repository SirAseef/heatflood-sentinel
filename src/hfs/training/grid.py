"""Grid track: ConvLSTM encoder-forecaster (FR-ML-05) and CNN grid heatwave detector (FR-ML-06).

Input: data/raw/openmeteo/grid_*.nc (CDS-like ERA5 lattice) or any CDS NetCDF dir via --raw.
python -m hfs.training.grid convlstm | cnn
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger

log = get_logger(__name__)


def load_grid(raw: Path | None = None):
    from hfs.features import units
    from hfs.ingestion import era5
    files = [raw] if raw and raw.is_file() else sorted((raw or config.path("raw_openmeteo")).glob("grid_*.nc"))
    ds = units.clean(era5.open_raw(files)).load()
    return ds


def split_masks(times: pd.DatetimeIndex, key: str = "grid"):
    sp = config.load("app")["splits"][key]
    return {s: (times >= a) & (times <= pd.Timestamp(b) + pd.Timedelta(hours=23)) for s, (a, b) in sp.items()}


def convlstm(raw: Path | None = None):
    import torch

    from hfs.benchmark.metrics import nse, rmse
    from hfs.models.convlstm import ConvLSTMForecaster, extreme_weighted_mse
    cfg = dict(config.load("models")["convlstm"])
    if os.environ.get("HFS_CONVLSTM_EPOCHS"):          # override for longer runs without editing the config
        cfg["epochs"] = int(os.environ["HFS_CONVLSTM_EPOCHS"])
    patience = int(cfg.get("patience", 0)) or None
    seed = config.load("models")["seed"]
    torch.manual_seed(seed)
    torch.set_num_threads(2)
    ds = load_grid(raw)
    times = pd.DatetimeIndex(ds.time.values)
    T = ds.t2m.values.astype("float32")                # (N, Y, X) degC
    P = np.log1p(ds.tp.values.astype("float32"))       # log1p mm/h
    m = split_masks(times)
    tr = m["train"]
    mu_t, sd_t = T[tr].mean(), T[tr].std()
    mu_p, sd_p = P[tr].mean(), P[tr].std()
    # hourly diurnal climatology per cell from TRAIN (doy +/-7 smoothing skipped: 1 train year -> use hour x month)
    key_tr = pd.DataFrame({"m": times.month[tr], "h": times.hour[tr]})
    clim_T = {k: T[tr][idx.values].mean(0) for k, idx in key_tr.groupby(["m", "h"]).groups.items()}
    clim_P = {k: np.expm1(P[tr][idx.values]).mean(0) for k, idx in key_tr.groupby(["m", "h"]).groups.items()}
    doy = times.dayofyear.values
    hr = times.hour.values
    ny, nx = T.shape[1:]
    cyc = np.stack([np.sin(2 * np.pi * doy / 365.25), np.cos(2 * np.pi * doy / 365.25),
                    np.sin(2 * np.pi * hr / 24), np.cos(2 * np.pi * hr / 24)], 1).astype("float32")
    X = np.concatenate([((T - mu_t) / sd_t)[:, None], ((P - mu_p) / sd_p)[:, None],
                        np.broadcast_to(cyc[:, :, None, None], (len(times), 4, ny, nx))], 1).astype("float32")
    L, H = cfg["lookback"], cfg["horizon"]

    def origins(mask, stride):
        idx = np.where(mask)[0]
        idx = idx[(idx >= L - 1) & (idx + H < len(times))]
        idx = idx[mask[np.minimum(idx + H, len(times) - 1)]]
        return idx[::stride]

    o_tr, o_va, o_te = origins(m["train"], cfg["train_stride_h"]), origins(m["val"], 6), origins(m["test"], 1)

    def batch(o):
        xb = np.stack([X[t - L + 1:t + 1] for t in o])
        yb = np.stack([X[t + 1:t + H + 1, :2] for t in o])
        return torch.from_numpy(xb), torch.from_numpy(yb)

    model = ConvLSTMForecaster(X.shape[1], 2, tuple(cfg["hidden"]), cfg["kernel"], H)
    opt = torch.optim.Adam(model.parameters(), lr=cfg["lr"])
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, factor=0.5, patience=1) if patience else None
    hist, best, best_state, bad = [], np.inf, None, 0
    t0 = time.time()
    rng = np.random.default_rng(seed)
    for ep in range(cfg["epochs"]):
        model.train()
        perm = rng.permutation(o_tr)
        tl = []
        for i in range(0, len(perm), cfg["batch"]):
            xb, yb = batch(perm[i:i + cfg["batch"]])
            opt.zero_grad()
            loss = extreme_weighted_mse(model(xb), yb, alpha=cfg["extreme_alpha"])
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            tl.append(loss.item())
        model.eval()
        with torch.no_grad():
            vl = float(np.mean([((model(xb) - yb) ** 2).mean().item()
                                for xb, yb in (batch(o_va[i:i + 64]) for i in range(0, len(o_va), 64))]))
        hist.append({"epoch": ep + 1, "train_loss": float(np.mean(tl)), "val_loss": vl})
        log.info("convlstm epoch %d train %.4f val %.4f", ep + 1, np.mean(tl), vl)
        if sched is not None:
            sched.step(vl)
        if vl < best:
            best, best_state, bad = vl, {k: v.clone() for k, v in model.state_dict().items()}, 0
        else:
            bad += 1
            if patience and bad >= patience:     # early stopping on the 2021 validation year
                log.info("convlstm early stop at epoch %d (best val %.4f)", ep + 1, best)
                break
    model.load_state_dict(best_state)
    fit_s = time.time() - t0
    # ---- test evaluation (per lead, all cells) vs persistence + climatology
    model.eval()
    preds = []
    t1 = time.time()
    with torch.no_grad():
        for i in range(0, len(o_te), 64):
            preds.append(model(batch(o_te[i:i + 64])[0]).numpy())
    infer_ms = (time.time() - t1) * 1000 / len(o_te)
    Pr = np.concatenate(preds)                         # (n, H, 2, Y, X) normalised
    rows = []
    for h in range(1, H + 1):
        tgt = o_te + h
        yT, yP = T[tgt], np.expm1(P[tgt])
        cT = Pr[:, h - 1, 0] * sd_t + mu_t
        cP = np.clip(np.expm1(Pr[:, h - 1, 1] * sd_p + mu_p), 0, None)
        pT, pP = T[o_te], np.expm1(P[o_te])
        kT = np.stack([clim_T[(times[t].month, times[t].hour)] for t in tgt])
        kP = np.stack([clim_P[(times[t].month, times[t].hour)] for t in tgt])
        for var, y, preds_ in (("t2m", yT, {"convlstm": cT, "persistence": pT, "climatology": kT}),
                               ("tp", yP, {"convlstm": cP, "persistence": pP, "climatology": kP})):
            for name, yh in preds_.items():
                rows.append({"var": var, "horizon": h, "model": name, "rmse": rmse(y.ravel(), yh.ravel()),
                             "nse": nse(y.ravel(), yh.ravel()), "n": int(y.size)})
    res = pd.DataFrame(rows)
    out = config.path("results")
    (out / "tables").mkdir(exist_ok=True)
    res.to_csv(out / "tables" / "grid_convlstm_test.csv", index=False)
    mdir = config.ROOT / "models"
    mdir.mkdir(exist_ok=True)
    torch.save(model.state_dict(), mdir / "convlstm_grid.pt")
    # full-grid 24-step forecast latency (NFR-PERF-04): unroll a 24-step model copy
    m24 = ConvLSTMForecaster(X.shape[1], 2, tuple(cfg["hidden"]), cfg["kernel"], 24)
    m24.load_state_dict(model.state_dict())
    xb = batch(o_te[:1])[0]
    t2 = time.time()
    with torch.no_grad():
        m24(xb)
    lat24 = (time.time() - t2) * 1000
    # spatial saliency (gradient x input) for the hottest test origin (FR-XAI-05)
    hot = o_te[np.argmax(T[o_te].mean((1, 2)))]
    xs = batch(np.array([hot]))[0].requires_grad_(True)
    model(xs)[:, :, 0].mean().backward()
    sal = (xs.grad * xs).abs().sum((0, 1, 2)).detach().numpy()
    np.save(out / "convlstm_saliency.npy", sal)
    # last test-window forecast for the map layer
    last = o_te[-1]
    with torch.no_grad():
        f = model(batch(np.array([last]))[0]).numpy()[0]
    meta = {"grid_shape": [int(ny), int(nx)], "lat": ds.lat.values.tolist(), "lon": ds.lon.values.tolist(),
            "train_origins": int(len(o_tr)), "test_origins": int(len(o_te)), "fit_seconds": round(fit_s, 1),
            "history": hist, "infer_ms_per_origin": infer_ms, "full_grid_24step_ms": lat24,
            "params": int(sum(p.numel() for p in model.parameters())),
            "saliency_time": str(times[hot]), "saliency": sal.tolist(),
            "map_issue_time": str(times[last]),
            "map_t2m_forecast_h6": (f[-1, 0] * sd_t + mu_t).round(2).tolist(),
            "map_t2m_obs_issue": T[last].round(2).tolist()}
    (out / "train_grid_convlstm.json").write_text(json.dumps(meta, indent=1, default=float))
    log.info("convlstm done fit %.0fs", fit_s)


def cnn(raw: Path | None = None):
    """Fully-convolutional CNN: last 3 days of daily Tmax anomaly/precip grids -> P(heatwave tomorrow) per cell."""
    import torch
    from torch import nn

    from hfs.benchmark.metrics import categorical, probabilistic
    from hfs.features import labels
    from hfs.models.calibration import fit_isotonic, tune_threshold
    seed = config.load("models")["seed"]
    torch.manual_seed(seed)
    hz = config.load("hazards")["heatwave"]
    ds = load_grid(raw)
    tmax = ds.t2m.resample(time="1D").max()
    tpd = ds.tp.resample(time="1D").sum()
    sp = config.load("app")["splits"]["grid"]
    base = (int(sp["train"][0][:4]), int(sp["train"][1][:4]))
    lab = labels.heatwave_labels(tmax, hz, base, hz["window_days"])["heatwave"].values.astype("float32")
    thr = labels.doy_percentile(tmax, hz["percentile"], base, hz["window_days"])
    anom = (tmax - thr.sel(dayofyear=tmax.time.dt.dayofyear)).values.astype("float32")
    days = pd.DatetimeIndex(tmax.time.values)
    X = np.stack([anom / 5.0, np.log1p(tpd.values) / 2.0], 1).astype("float32")
    K = 3
    ms = split_masks(days)

    def make(mask):
        idx = np.where(mask)[0]
        idx = idx[(idx >= K - 1) & (idx + 1 < len(days))]
        idx = idx[mask[idx + 1]]
        xb = np.stack([X[t - K + 1:t + 1].reshape(-1, *X.shape[2:]) for t in idx])
        return torch.from_numpy(xb), torch.from_numpy(lab[idx + 1][:, None]), idx

    net = nn.Sequential(nn.Conv2d(2 * K, 16, 3, padding=1), nn.ReLU(), nn.Conv2d(16, 16, 3, padding=1), nn.ReLU(),
                        nn.Conv2d(16, 1, 1))
    xtr, ytr, _ = make(ms["train"])
    xva, yva, _ = make(ms["val"])
    xte, yte, ite = make(ms["test"])
    pos = float(ytr.mean())

    def focal(logit, y, gamma=2.0, alpha=0.75):
        p = torch.sigmoid(logit)
        pt = torch.where(y > 0, p, 1 - p)
        a = torch.where(y > 0, torch.tensor(alpha), torch.tensor(1 - alpha))
        return (-a * (1 - pt) ** gamma * torch.log(pt.clamp(1e-6))).mean()

    opt = torch.optim.Adam(net.parameters(), lr=3e-3)
    t0 = time.time()
    for _ep in range(150):
        net.train()
        opt.zero_grad()
        loss = focal(net(xtr), ytr)
        loss.backward()
        opt.step()
    net.eval()
    with torch.no_grad():
        pva = torch.sigmoid(net(xva)).numpy().ravel()
        pte = torch.sigmoid(net(xte)).numpy().ravel()
    iso = fit_isotonic(pva, yva.numpy().ravel())
    pva_c, pte_c = iso.predict(pva), iso.predict(pte)
    th = tune_threshold(pva_c, yva.numpy().ravel(), config.load("models")["detection"]["far_cap"])
    yt = yte.numpy().ravel()
    res = {"model": "cnn_grid", "hazard": "heatwave", "lead_days": 1, "train_pos_rate": pos,
           "test_pos_rate": float(yt.mean()), "threshold": th["threshold"], "fit_seconds": round(time.time() - t0, 1),
           **categorical(yt, pte_c >= th["threshold"]), **probabilistic(yt, pte_c),
           "rule_persistence": categorical(yt, lab[ite].ravel() > 0)}
    out = config.path("results")
    (out / "train_grid_cnn.json").write_text(json.dumps(res, indent=1, default=float))
    torch.save(net.state_dict(), config.ROOT / "models" / "cnn_grid_heatwave.pt")
    log.info("cnn done %s", res)


if __name__ == "__main__":
    {"convlstm": convlstm, "cnn": cnn}[sys.argv[1]](Path(sys.argv[2]) if len(sys.argv) > 2 else None)
