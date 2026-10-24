# Contributing

Thanks for your interest in HeatFlood Sentinel. This repository is the artefact of an MS thesis, so changes that
affect reported results need to keep the evaluation honest and reproducible.

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install --index-url https://download.pytorch.org/whl/cpu torch   # CPU is enough
make setup
make test lint
```

## Ground rules

1. **No data in git.** Never commit ERA5/PMD files, model binaries (`models/*.pt`, `models/*.json`), logs or `.env`.
   Tiny synthetic fixtures for tests are fine.
2. **Time-only splits.** Do not fit anything (scalers, climatologies, thresholds, calibration) on 2022 test data.
3. **Train–serve parity.** Feature logic lives in `hfs.features`; the stream processor and training both call it.
   Do not duplicate feature code in a runtime adapter.
4. **Baselines stay in every table**, including where they win. Do not hand-edit anything under `results/`; rerun
   `make benchmark report`.
5. **Config over code.** Hazard definitions, alert levels, splits and hyper-parameters belong in `configs/*.yaml`.
6. Document any new caveat in `DEVIATIONS.md` and user-visible changes in `CHANGELOG.md`.

## Style and checks

* `ruff check src tests scripts` (line length 170, rules E, F, W, I, B) must pass.
* `pytest` must pass offline; new logic needs a unit test on synthetic data (`tests/conftest.py` has generators).
* Type hints on public functions; docstrings say units (°C, mm, mm/h) and time zone (UTC storage, PKT days).

## Pull requests

* Branch from `main`, keep PRs focused, and describe what changed and how you verified it.
* If a PR changes metrics, include the before/after rows from `results/RESULTS.md`.
* CI runs lint → tests → Docker build.

## Reporting issues

Open a GitHub issue with steps to reproduce, the `make` target, your OS/Python version and the relevant log
lines (`logs/` is local only).
