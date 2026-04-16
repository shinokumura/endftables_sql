"""
Parser for the evaluated resonance parameters stored in https://github.com/arjankoning1/resonancetables
Load Maxwellian XS (MACS), thermal cross sections, and resonance parameters
from the resonancetables directory into the SQLite database.

Source files are in the `all/` subdirectories.  Year metadata is read from the
per-nuclide `nuc/` subdirectories, where each data row has the column layout:
    Author  Type  Year  Value  dValue  Reference  Ratio  ...

Experimental data are not loaded here; they come from exforparser stored in
https://github.com/shinokumura/resonance_data  and
https://github.com/shinokumura/thermaldata

Integration with endf_reactions
--------------------------------
Each unique (source, data_type, nuclide, process) gets one row in endf_reactions:
    evaluation = source        e.g. "Mughabghab-2018", "selected"
    obs_type   = data_type     "macs" | "thermal" | RESONANCE_PARAMS
    target     = nuclide       e.g. "U235", "Am242m"
    projectile = "n"
    process    = outgoing channel: "g", "f", "el", "a", "p", "tot" (NULL for resonance_param)
    year       = publication year of that source, parsed from nuc/ files

resonancetable_data then stores one row per (reaction_id, quantity):
    reaction_id → endf_reactions.reaction_id
    quantity    = the observable code, e.g. "ng", "D0", "gamgam0"
"""

import re
import pandas as pd
import sqlalchemy as sa
from pathlib import Path

from endftables_sql.config import engines, RESONANCETABLES_PATH, BATCH_SIZE
from endftables_sql.submodules.utilities.elem import elemtoz, ztoelem

RT_TABLE   = "resonancetable_data"
IDX_TABLE  = "endf_reactions"

# Resonance parameter directories
RESONANCE_PARAMS = ["D0", "D1", "D2", "If", "Ig", "R", "S0", "S1", "gamgam0", "gamgam1"]

# quantity → (process_code, residual_fn(z, a) → (rZ, rA, suffix_str) or None)
# process_code mirrors existing ENDF process column values: "g", "f", "el", "a", "p", "tot"
# suffix_str is the isomer suffix appended to the residual nuclide string:
#   ""  → plain ground state (e.g. "Al028")
#   "g" → explicit ground state (e.g. "Al028g", used when -g variant is specified)
#   "m" → metastable (e.g. "Al028m")
# Each quantity gets its own endf_reactions row (unique residual distinguishes them).
_QTY_REACTION = {
    "ng":    ("g",   lambda z, a: (z,   a+1, "")),
    "ng-g":  ("g",   lambda z, a: (z,   a+1, "g")),
    "ng-m":  ("g",   lambda z, a: (z,   a+1, "m")),
    "ng-n":  ("g",   lambda z, a: (z,   a+1, "")),   # same residual as ng; separate via obs_type
    "nf":    ("f",   None),
    "el":    ("el",  lambda z, a: (z,   a,   "")),
    "na":    ("a",   lambda z, a: (z-2, a-3, "")),
    "na-g":  ("a",   lambda z, a: (z-2, a-3, "g")),
    "na-m":  ("a",   lambda z, a: (z-2, a-3, "m")),
    "np":    ("p",   lambda z, a: (z-1, a,   "")),
    "tot":   ("tot", None),
}


# ---------------------------------------------------------------------------
# Nuclide helpers
# ---------------------------------------------------------------------------

def _parse_nuclide(nuclide_str: str):
    """
    Parse libstyle nuclide string.
    'U235' → ('U', 92, 235, 0)   'Am242m' → ('Am', 95, 242, 1)
    """
    m = re.match(r"([A-Za-z]+)(\d+)(m\d*)?", nuclide_str)
    if not m:
        return None
    elem = m.group(1).capitalize()
    mass = int(m.group(2))
    liso = int(m.group(3)[1:]) if m.group(3) and len(m.group(3)) > 1 else (1 if m.group(3) else 0)
    try:
        z = int(elemtoz(elem))
    except Exception:
        z = 0
    return elem, z, mass, liso


def _format_nuclide(elem: str, mass: int, suffix: str = "") -> str:
    """Format as libstyle string: elem + 3-digit mass + isomer suffix.
    suffix: ""  → plain (e.g. "Al028")
            "g" → explicit ground state (e.g. "Al028g")
            "m" → metastable (e.g. "Al028m")
    """
    return f"{elem.capitalize()}{str(mass).zfill(3)}{suffix}"


def _calc_process_residual(nuclide_str: str, quantity: str):
    """
    Return (process, residual) for an endf_reactions row.
    Both may be None for reactions without a unique residual.
    """
    entry = _QTY_REACTION.get(quantity)
    if entry is None:
        return None, None

    process, res_fn = entry
    if res_fn is None:
        return process, None

    parsed = _parse_nuclide(nuclide_str)
    if parsed is None:
        return process, None

    elem, z, mass, _ = parsed
    rz, ra, suffix = res_fn(z, mass)
    try:
        r_elem = ztoelem(rz).capitalize()
    except Exception:
        return process, None

    return process, _format_nuclide(r_elem, ra, suffix)


# ---------------------------------------------------------------------------
# File parser
# ---------------------------------------------------------------------------

def _read_ncols(filepath: Path) -> int:
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
    Parse a resonancetable all/*.txt file.

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
                "nuclide": nuclide, "source": source,
                "data_type": data_type, "quantity": quantity,
                "value": value, "dvalue": dvalue,
                "rel_dev_comp": rd_comp, "rel_dev_ndl": rd_ndl,
                "rel_dev_exfor": rd_exfor, "rel_dev_all": rd_all,
                "n_exper": n_exper, "spectrum": spectrum,
            })

    return pd.DataFrame(data_rows)


def _build_year_map(base: Path) -> dict:
    """
    Scan every nuc/ file and return a mapping
        (source, data_type, nuclide) → year (int)

    nuc/ files have the column layout (cols 0–4 are always present):
        Author  Type  Year  Value  dValue  ...
    The nuclide is taken from the '# nuclide:' header line.
    Year is always at column index 2.
    """
    year_map: dict = {}

    def _scan_nuc_dir(data_type: str, nuc_dir: Path):
        if not nuc_dir.exists():
            return
        for f in nuc_dir.glob("*.txt"):
            nuclide = None
            with open(f, encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    line = line.rstrip()
                    if line.startswith("#") and "nuclide:" in line:
                        nuclide = line.split("nuclide:")[-1].strip()
                        continue
                    # Skip all comment and blank lines (includes ## header rows)
                    if line.startswith("#") or not line:
                        continue
                    if nuclide is None:
                        continue
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    try:
                        source = parts[0]
                        year   = int(parts[2])
                        key = (source, data_type, nuclide)
                        # Keep the first occurrence (highest in the file = primary value)
                        if key not in year_map:
                            year_map[key] = year
                    except (ValueError, IndexError):
                        continue

    _scan_nuc_dir("macs", base / "macs" / "ng" / "nuc")
    for rxn in _QTY_REACTION:
        _scan_nuc_dir("thermal", base / "thermal" / rxn / "nuc")
    for param in RESONANCE_PARAMS:
        # If/Ig → obs_type "integral"; all others use the param name itself
        obs = "integral" if param in ("If", "Ig") else param
        _scan_nuc_dir(obs, base / "resonance" / param / "nuc")

    return year_map


def extract_source_from_filename(filename: str, quantity: str) -> str:
    stem = Path(filename).stem
    suffix = f"_{quantity}"
    if stem.endswith(suffix):
        return stem[: -len(suffix)]
    return stem.split("_")[0]


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_all_to_db(engine=None):
    """
    Load all resonancetable data from the `all/` subdirectories into the DB.

    Strategy
    --------
    1. Parse every file into a combined DataFrame (nuclide, source, data_type,
       quantity, value, …).
    2. Remove any existing resonancetable rows from endf_reactions and
       resonancetable_data.
    3. Build the reaction index: one endf_reactions row per unique
       (source, data_type, nuclide).  The process/residual columns are
       populated for thermal reactions that have a well-defined product.
    4. Query back the assigned reaction_ids and merge into the data frame.
    5. Insert resonancetable_data rows (reaction_id, quantity, value, …).
    """
    if engine is None:
        engine = engines["endftables"]

    base = Path(RESONANCETABLES_PATH)
    all_rows = []

    def _collect(data_type: str, quantity: str, dirpath: Path):
        if not dirpath.exists():
            return
        for f in sorted(dirpath.glob("*.txt")):
            source = extract_source_from_filename(f.name, quantity)
            print(f"  {data_type:20s}  {quantity:12s}  {source}")
            df = parse_resonancetable_file(f, data_type, quantity, source)
            if not df.empty:
                all_rows.append(df)

    # --- MACS ---
    _collect("macs", "ng", base / "macs" / "ng" / "all")

    # --- Thermal XS ---
    for rxn in _QTY_REACTION:
        _collect("thermal", rxn, base / "thermal" / rxn / "all")

    # --- Resonance Parameters ---
    for param in RESONANCE_PARAMS:
        _collect("resonance_param", param, base / "resonance" / param / "all")

    if not all_rows:
        print("No data collected.")
        return 0

    data_df = pd.concat(all_rows, ignore_index=True)

    # ------------------------------------------------------------------
    # Derive per-row columns for the endf_reactions key
    #
    # obs_type:  thermal/macs → data_type as-is
    #            resonance_param → quantity itself (D0, S0, Ig, …)
    # process:   thermal/macs → from _QTY_REACTION ("g", "f", "el", …)
    #            resonance_param → None
    # residual:  computed from (nuclide, quantity) for thermal/macs; None otherwise
    # ------------------------------------------------------------------
    def _row_obs_type(r):
        if r["data_type"] == "resonance_param":
            return "integral" if r["quantity"] in ("If", "Ig") else r["quantity"]
        return r["data_type"]

    def _row_process(r):
        if r["data_type"] in ("thermal", "macs"):
            return _QTY_REACTION[r["quantity"]][0] if r["quantity"] in _QTY_REACTION else None
        if r["data_type"] == "resonance_param" and r["quantity"] in ("If", "Ig"):
            return "f" if r["quantity"] == "If" else "g"
        return None

    def _row_residual(r):
        if r["data_type"] not in ("thermal", "macs"):
            return None
        try:
            return _calc_process_residual(r["nuclide"], r["quantity"])[1]
        except Exception:
            return None

    data_df["obs_type"]  = data_df.apply(_row_obs_type,  axis=1)
    data_df["process"]   = data_df.apply(_row_process,   axis=1)
    data_df["residual"]  = data_df.apply(_row_residual,  axis=1)

    # ------------------------------------------------------------------
    # All obs_types managed by this loader.
    # "If"/"Ig" listed for backward-compat DELETE; new rows use "integral".
    # ------------------------------------------------------------------
    _PARAM_OBS = [p for p in RESONANCE_PARAMS if p not in ("If", "Ig")] + ["integral"]
    _ALL_OBS   = ["macs", "thermal", "resonance_param", "If", "Ig"] + _PARAM_OBS
    _obs_in    = ", ".join(f"'{t}'" for t in _ALL_OBS)

    # ------------------------------------------------------------------
    # Clear existing resonancetable entries
    # ------------------------------------------------------------------
    with engine.begin() as conn:
        conn.execute(sa.text(f"DELETE FROM {IDX_TABLE} WHERE obs_type IN ({_obs_in})"))
        conn.execute(sa.text(f"DELETE FROM {RT_TABLE}"))

    # ------------------------------------------------------------------
    # Build reaction index: one row per (source, obs_type, nuclide, process, residual)
    # thermal/macs: ng / ng-g / ng-m each get their own row via unique residual
    # resonance:    one row per param per nuclide per source (obs_type = param name)
    # ------------------------------------------------------------------
    idx_df = (
        data_df[["source", "obs_type", "nuclide", "process", "residual"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # Year from nuc/ files — key matches obs_type used above
    # ------------------------------------------------------------------
    year_map = _build_year_map(base)
    idx_df["year"] = idx_df.apply(
        lambda r: year_map.get((r["source"], r["obs_type"], r["nuclide"])),
        axis=1,
    ).astype("Int64")

    idx_df["evaluation"] = idx_df["source"]
    idx_df["target"]     = idx_df["nuclide"]
    idx_df["projectile"] = "n"

    insert_cols = ["evaluation", "obs_type", "target", "projectile", "process", "residual", "year"]
    idx_df[insert_cols].to_sql(
        IDX_TABLE, con=engine, if_exists="append", index=False, chunksize=BATCH_SIZE
    )

    # ------------------------------------------------------------------
    # Query back reaction_ids; key = (evaluation, obs_type, target, process, residual)
    # ------------------------------------------------------------------
    with engine.connect() as conn:
        id_rows = conn.execute(sa.text(
            f"SELECT reaction_id, evaluation, obs_type, target, process, residual "
            f"FROM {IDX_TABLE} WHERE obs_type IN ({_obs_in})"
        )).fetchall()

    id_map = {(r.evaluation, r.obs_type, r.target, r.process, r.residual): r.reaction_id
              for r in id_rows}

    data_df["reaction_id"] = data_df.apply(
        lambda r: id_map.get(
            (r["source"], r["obs_type"], r["nuclide"], r["process"], r["residual"])
        ),
        axis=1,
    )

    # ------------------------------------------------------------------
    # Insert resonancetable_data  (quantity column removed — obs_type in
    # endf_reactions now carries that information)
    # ------------------------------------------------------------------
    rt_cols = [
        "reaction_id", "value", "dvalue",
        "rel_dev_comp", "rel_dev_ndl", "rel_dev_exfor", "rel_dev_all",
        "n_exper", "spectrum",
    ]
    data_df[rt_cols].to_sql(
        RT_TABLE, con=engine, if_exists="append", index=False, chunksize=BATCH_SIZE
    )

    total = len(data_df)
    print(f"\nLoaded {total} rows into {RT_TABLE} "
          f"({len(idx_df)} reaction index rows in {IDX_TABLE}).")
    return total


def create_indexes(engine=None):
    """Create query indexes on resonancetable_data and the new endf_reactions rows."""
    if engine is None:
        engine = engines["endftables"]

    with engine.begin() as conn:
        conn.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS idx_rt_reaction_id "
            "ON resonancetable_data (reaction_id)"
        ))
        conn.execute(sa.text(
            "CREATE INDEX IF NOT EXISTS idx_endf_obs_target "
            "ON endf_reactions (obs_type, target)"
        ))
    print("Indexes created.")


if __name__ == "__main__":
    load_all_to_db()
    create_indexes()
