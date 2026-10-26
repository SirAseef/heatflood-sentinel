"""Idempotent compatibility patch: PySpark 3.5.x applyInPandasWithState with pandas >= 3.

With pandas 3 the per-group state-info struct comes back from Arrow with float values, and
`RecordBatch.slice(startOffset, numRows)` raises "TypeError: 'float' object cannot be interpreted as an integer"
(pyspark/sql/pandas/serializers.py, ApplyInPandasWithStateSerializer.load_stream). The patch casts both to int.
Patched in SPARK_HOME/python (driver / local mode) and in SPARK_HOME/python/lib/pyspark.zip (shipped to YARN executors).
Upstream fix is in later Spark releases; this is recorded in DEVIATIONS.md (D-BD).
"""
import os
import shutil
import sys
import zipfile
from pathlib import Path

OLD = ['data_start_offset = state_info_col["startOffset"]', 'num_data_rows = state_info_col["numRows"]']
NEW = ['data_start_offset = int(state_info_col["startOffset"])', 'num_data_rows = int(state_info_col["numRows"])']
REL = "pyspark/sql/pandas/serializers.py"


def patch_text(s: str) -> tuple[str, bool]:
    changed = False
    for o, n in zip(OLD, NEW):
        if o in s and n not in s:
            s = s.replace(o, n)
            changed = True
    return s, changed


def main():
    home = Path(os.environ.get("SPARK_HOME", str(Path.home() / "bigdata" / "spark"))).resolve()
    f = home / "python" / REL
    s, ch = patch_text(f.read_text())
    if ch:
        shutil.copy(f, str(f) + ".orig")
        f.write_text(s)
    print(f"{f}: {'patched' if ch else 'already patched'}")
    z = home / "python" / "lib" / "pyspark.zip"
    with zipfile.ZipFile(z) as zin:
        s2 = zin.read(REL).decode()
        s2, ch2 = patch_text(s2)
        if ch2:
            tmp = z.with_suffix(".zip.tmp")
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    data = s2.encode() if item.filename == REL else zin.read(item.filename)
                    zout.writestr(item, data)
    if ch2:
        shutil.copy(z, str(z) + ".orig")
        tmp.replace(z)
    print(f"{z}: {'patched' if ch2 else 'already patched'}")


if __name__ == "__main__":
    sys.exit(main())
