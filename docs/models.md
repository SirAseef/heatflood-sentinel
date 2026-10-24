# Models

Scope follows the thesis proposal: two baseline families (SARIMA, XGBoost) and two deep families (LSTM, ConvLSTM),
plus naive persistence / climatology references and hazard detectors. Hyper-parameters live in `configs/models.yaml`.

| Track | Target | Models | Notes |
|---|---|---|---|
| Hourly (`make train-hourly`) | t2m, precipitation at +1, +6, +24 h, 20 cities | persistence, diurnal persistence, climatology, XGBoost (one per lead), LSTM (multi-horizon, city embedding) | 140 features: lags, rolling stats, anomalies vs train climatology, cyclic time, elevation |
| Daily (`make train-daily`) | daily Tmax and precipitation, leads 1–7 d | persistence, climatology, SARIMA (1,0,1 + Fourier k = 3, per city), XGBoost, LSTM (3 seeds) | precipitation modelled in log1p space |
| Grid (`make train-grid`) | t2m, precipitation on 2° grid, leads 1–6 h | ConvLSTM encoder–forecaster (2 × 16 ch, k = 3, lookback 12 h, residual decoder, extreme-weighted MSE); persistence; hour × month climatology | in-house ConvLSTM cell (no external package), early stopping on 2021 |
| Grid detector | heatwave cell-days, lead 1 d | small CNN on Tmax anomaly + log precipitation with focal loss | vs rule persistence |
| Detection (`make detect`) | heatwave lead 1/3 d; flood proxy lead 6/24 h | rule persistence, LSTM-forecast + rule, XGBoost classifier | isotonic calibration on validation; threshold maximising CSI subject to FAR ≤ 0.5 |

## Explainability

* TreeSHAP contributions for every XGBoost detector, stored per alert as `drivers` and summarised globally in
  `results/explain_global.json` (Explainability Lab page).
* Plain-language "why did this fire" narratives (`alert_engine.narrative`).
* ConvLSTM input saliency map (`results/convlstm_saliency.npy`).

## Artefacts

`make train` writes model files to `models/` (git-ignored: `*.pt`, XGBoost `*.json`) and run records to
`results/train_*.json`. Released numbers come from these run records via `make report`.
