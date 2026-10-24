"""Derived indices: relative humidity, heat index (NWS), antecedent precipitation index."""
import numpy as np
import xarray as xr


def relative_humidity(t_c, td_c):
    """Magnus formula (SRS 7.3). Inputs in degC, output in %."""
    a, b = 17.625, 243.04
    rh = 100.0 * np.exp(a * td_c / (b + td_c)) / np.exp(a * t_c / (b + t_c))
    return np.clip(rh, 0.0, 100.0)


def heat_index_c(t_c, rh):
    """NWS heat index. Steadman below 80 F, Rothfusz regression above, with NWS adjustments.

    Works on numpy arrays or xarray objects. Returns degC.
    """
    T = t_c * 9.0 / 5.0 + 32.0
    RH = rh
    simple = 0.5 * (T + 61.0 + (T - 68.0) * 1.2 + RH * 0.094)
    hi = (-42.379 + 2.04901523 * T + 10.14333127 * RH - 0.22475541 * T * RH
          - 0.00683783 * T * T - 0.05481717 * RH * RH + 0.00122874 * T * T * RH
          + 0.00085282 * T * RH * RH - 0.00000199 * T * T * RH * RH)
    adj1 = ((13.0 - RH) / 4.0) * np.sqrt(np.maximum(17.0 - np.abs(T - 95.0), 0.0) / 17.0)
    hi = xr.where((RH < 13) & (T >= 80) & (T <= 112), hi - adj1, hi) if isinstance(hi, xr.DataArray) \
        else np.where((RH < 13) & (T >= 80) & (T <= 112), hi - adj1, hi)
    adj2 = ((RH - 85.0) / 10.0) * ((87.0 - T) / 5.0)
    cond2 = (RH > 85) & (T >= 80) & (T <= 87)
    hi = xr.where(cond2, hi + adj2, hi) if isinstance(hi, xr.DataArray) else np.where(cond2, hi + adj2, hi)
    use_simple = 0.5 * (simple + T) < 80.0
    final = xr.where(use_simple, simple, hi) if isinstance(hi, xr.DataArray) else np.where(use_simple, simple, hi)
    return (final - 32.0) * 5.0 / 9.0


def antecedent_precip_index(daily_p: xr.DataArray, k: float = 0.85) -> xr.DataArray:
    """API_t = k * API_{t-1} + P_t on a daily time axis (SRS 7.3)."""
    vals = daily_p.fillna(0.0).values
    out = np.empty_like(vals, dtype="float32")
    prev = np.zeros(vals.shape[1:], dtype="float32")
    for t in range(vals.shape[0]):
        prev = k * prev + vals[t]
        out[t] = prev
    return xr.DataArray(out, coords=daily_p.coords, dims=daily_p.dims, name="api")
