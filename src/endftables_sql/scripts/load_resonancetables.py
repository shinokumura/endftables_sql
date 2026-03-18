"""
Parser for the evaluated resonance parameters stored in https://github.com/arjankoning1/resonancetables
Load Maxwellian XS (MACS), thermal cross sections, and resonance parameters
from the resonancetables directory into the SQLite database.

Source files are in the `all/` subdirectories only.
Individual per-nuclide `nuc/` directories are not used.
Experimental data are not used since they comes from exforparser stored in 
https://github.com/shinokumura/resonance_data
and
https://github.com/shinokumura/thermaldata
"""

import pandas as pd
from pathlib import Path

from endftables_sql.config import engines, RESONANCETABLES_PATH, BATCH_SIZE

TABLE_NAME = "resonancetable_data"

# Resonance parameter directories
RESONANCE_PARAMS = ["D0", "D1", "D2", "If", "Ig", "R", "S0", "S1", "gamgam0", "gamgam1"]

# Thermal cross-section reaction types
THERMAL_REACTIONS = ["el", "na", "na-g", "na-m", "nf", "ng", "ng-g", "ng-m", "ng-n", "np", "nu", "nud", "nup", "tot"]


def _read_ncols(filepath: Path) -> int:
    """Read the declared column count from the file header."""
    with open(filepath, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") and "columns:" in line:
                try:
                    return int(line.split("columns:")[-1].strip())
                except ValueError:
                    pass
    return 0


def parse_resonancetable_file(filepath: Path, data_type: str, quantity: str, source: str) -> pd.DataFrame:
    """
    Parse a resonancetable all/*.txt file into a DataFrame.

    Three data-line formats (detected from header '# columns: N'):
      13 cols — selected MACS/thermal:
        Z A Liso Value dValue Ref rd_comp rd_ndl rd_exfor rd_all n_exper Spectrum Nuclide
      12 cols — selected resonance params (no Spectrum):
        Z A Liso Value dValue Ref rd_comp rd_ndl rd_exfor rd_all n_exper Nuclide
       7 cols — per-source compilation/NDL files:
        Z A Liso Value dValue Ratio Nuclide
      10 cols — EXFOR individual measurements (skipped).
    """
    ncols = _read_ncols(filepath)
    if ncols == 10:
        # EXFOR individual measurements — too granular, skip
        return pd.DataFrame()

    data_rows = []
    with open(filepath, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()

            try:
                if ncols == 13:
                    # selected MACS/thermal: may have 12 parts when Spectrum is blank
                    if len(parts) < 12:
                        continue
                    z, a, liso = int(parts[0]), int(parts[1]), int(parts[2])
                    value, dvalue = float(parts[3]), float(parts[4])
                    rd_comp, rd_ndl, rd_exfor, rd_all = (
                        float(parts[6]), float(parts[7]), float(parts[8]), float(parts[9])
                    )
                    n_exper = int(parts[10])
                    if len(parts) == 12:
                        spectrum, nuclide = None, parts[11]
                    else:
                        spectrum = parts[11] if parts[11] not in ("", " ") else None
                        nuclide = parts[12]

                elif ncols == 12:
                    # selected resonance params (no Spectrum col)
                    if len(parts) < 12:
                        continue
                    z, a, liso = int(parts[0]), int(parts[1]), int(parts[2])
                    value, dvalue = float(parts[3]), float(parts[4])
                    rd_comp, rd_ndl, rd_exfor, rd_all = (
                        float(parts[6]), float(parts[7]), float(parts[8]), float(parts[9])
                    )
                    n_exper = int(parts[10])
                    nuclide, spectrum = parts[11], None

                elif ncols == 7:
                    # per-source compilation / NDL file
                    if len(parts) < 7:
                        continue
                    z, a, liso = int(parts[0]), int(parts[1]), int(parts[2])
                    value, dvalue = float(parts[3]), float(parts[4])
                    rd_comp = rd_ndl = rd_exfor = rd_all = None
                    n_exper = None
                    nuclide, spectrum = parts[6], None

                else:
                    continue

            except (ValueError, IndexError):
                continue

            data_rows.append({
                "z": z, "a": a, "liso": liso, "nuclide": nuclide,
                "data_type": data_type, "quantity": quantity, "source": source,
                "value": value, "dvalue": dvalue,
                "rel_dev_comp": rd_comp, "rel_dev_ndl": rd_ndl,
                "rel_dev_exfor": rd_exfor, "rel_dev_all": rd_all,
                "n_exper": n_exper, "spectrum": spectrum,
            })

    return pd.DataFrame(data_rows)


def extract_source_from_filename(filename: str, quantity: str) -> str:
    """
    Extract source name from filename pattern: {source}_{quantity}.txt
    E.g. "Mughabghab-2018_ng.txt" → "Mughabghab-2018"
         "selected_D0.txt" → "selected"
    """
    stem = Path(filename).stem          # e.g. "Mughabghab-2018_ng"
    suffix = f"_{quantity}"
    if stem.endswith(suffix):
        return stem[: -len(suffix)]
    # fallback: everything before first underscore
    return stem.split("_")[0]


def load_all_to_db(engine=None):
    """
    Load all resonancetable data from the `all/` subdirectories into the DB.
    Clears existing resonancetable_data rows before reloading.
    """
    if engine is None:
        engine = engines["endftables"]

    base = Path(RESONANCETABLES_PATH)

    with engine.begin() as conn:
        conn.execute(__import__("sqlalchemy").text(f"DELETE FROM {TABLE_NAME}"))

    total = 0

    def _insert(df: pd.DataFrame):
        nonlocal total
        if df.empty:
            return
        df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False, chunksize=BATCH_SIZE)
        total += len(df)

    # --- MACS ---
    macs_all = base / "macs" / "ng" / "all"
    if macs_all.exists():
        for f in sorted(macs_all.glob("*.txt")):
            source = extract_source_from_filename(f.name, "macs")
            print(f"  MACS  ng  {source}")
            df = parse_resonancetable_file(f, data_type="macs", quantity="ng", source=source)
            _insert(df)

    # --- Thermal XS ---
    for rxn in THERMAL_REACTIONS:
        thermal_all = base / "thermal" / rxn / "all"
        if not thermal_all.exists():
            continue
        for f in sorted(thermal_all.glob("*.txt")):
            source = extract_source_from_filename(f.name, rxn)
            print(f"  thermal  {rxn}  {source}")
            df = parse_resonancetable_file(f, data_type="thermal", quantity=rxn, source=source)
            _insert(df)

    # --- Resonance Parameters ---
    for param in RESONANCE_PARAMS:
        res_all = base / "resonance" / param / "all"
        if not res_all.exists():
            continue
        for f in sorted(res_all.glob("*.txt")):
            source = extract_source_from_filename(f.name, param)
            print(f"  resonance_param  {param}  {source}")
            df = parse_resonancetable_file(f, data_type="resonance_param", quantity=param, source=source)
            _insert(df)

    print(f"\nLoaded {total} rows into {TABLE_NAME}.")
    return total


def create_indexes(engine=None):
    """Create query indexes on resonancetable_data."""
    if engine is None:
        engine = engines["endftables"]
    import sqlalchemy as sa
    with engine.begin() as conn:
        conn.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS idx_rt_nuclide ON resonancetable_data (nuclide)"
        ))
        conn.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS idx_rt_type_qty ON resonancetable_data (data_type, quantity)"
        ))
        conn.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS idx_rt_z_a ON resonancetable_data (z, a, liso)"
        ))
    print("Indexes created on resonancetable_data.")


if __name__ == "__main__":
    load_all_to_db()
    create_indexes()
