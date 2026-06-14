"""
Load MF=6 double-differential/spectrum tables from ENDF-6 files.

The parser follows the DeCEUtilities MF6 workflow:
    dece -s FILE           -> find available MF6 MT numbers
    dece -f6 -t MT FILE    -> tabulate incident energy, outgoing energy, spectrum/PL values

Only light outgoing particles handled by the original utility are loaded:
neutron, proton, deuteron, triton, helium-3, and alpha.

When DeCE prints Legendre coefficients, this loader reconstructs angle-resolved
DDX data on a configurable angle grid and stores rows as:
    incident energy, angle, outgoing energy, value.
Spectrum-only MF6 sections are kept with angle=NULL.
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
from sqlalchemy import and_, insert, select, text, update
from endftables_sql.config import engines, LIB_LIST_FILE, ENDF_ARCHIVE_PATH, DECE_EXECUTABLE, PROJECTILE_ALIASES, PROJECTILE_DIRS, LIGHT_PARTICLES, ENDF_SUFFIXES
from endftables_sql.scripts.models_core import endf_ddx_data, endf_reactions
from endftables_sql.submodules.utilities.reaction import mt_to_process

DDX_OBS_TYPE = "ddx"
DEFAULT_ANGLE_STEP_DEG = 5.0
DEFAULT_MF6_MTS = (
    16, 17, 22, 28, 37, 91, 103, 104, 105, 106, 107,
    600, 649, 650, 699, 700, 749, 750, 799, 800, 849,
)

LOGGER = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", action="append", help="evaluation key in LIB_LIST_FILE")
    parser.add_argument("--projectile", action="append", help="projectile directory to load")
    parser.add_argument("--target", action="append", help="target in DB style, e.g. Ni060")
    parser.add_argument("--mt", action="append", type=int, help="restrict to one or more MT numbers")
    parser.add_argument("--archive-path", default=ENDF_ARCHIVE_PATH)
    parser.add_argument("--dece", default=DECE_EXECUTABLE)
    parser.add_argument("--replace", action="store_true", help="delete matching DDX rows before loading")
    parser.add_argument("--dry-run", action="store_true", help="scan and parse without writing database rows")
    parser.add_argument("--limit", type=int, help="maximum ENDF files per library/projectile")
    parser.add_argument(
        "--angle-step",
        type=float,
        default=DEFAULT_ANGLE_STEP_DEG,
        help="angle grid spacing in degrees for Legendre MF6 DDX reconstruction",
    )
    parser.add_argument(
        "--angles",
        help="comma-separated angle grid in degrees; overrides --angle-step",
    )
    return parser.parse_args()


def target_from_endf_filename(path: Path) -> str | None:
    match = re.search(r"_(\d{1,3})-([A-Za-z]+)-(\d+)([MmNnGg]?)(?=[_.])", path.name)
    if not match:
        return None
    _, elem, mass, iso = match.groups()
    suffix = iso.lower()
    return f"{elem.capitalize()}{int(mass):03d}{suffix}"


def projectile_from_dir(dirname: str) -> str:
    return PROJECTILE_ALIASES.get(dirname, dirname)


def iter_endf_files(archive_path: str, libraries: Iterable[str], projectiles: set[str] | None):
    root = Path(archive_path)
    for evaluation, dirname in LIB_LIST_FILE.items():
        if libraries and evaluation not in libraries:
            continue

        lib_dir = root / dirname
        if not lib_dir.exists():
            LOGGER.warning("library directory missing: %s", lib_dir)
            continue

        for projectile_dir in sorted(p for p in lib_dir.iterdir() if p.is_dir()):
            if projectile_dir.name not in PROJECTILE_DIRS:
                continue

            projectile = projectile_from_dir(projectile_dir.name)
            if projectiles and projectile not in projectiles and projectile_dir.name not in projectiles:
                continue

            for endf_file in sorted(p for p in projectile_dir.rglob("*") if p.suffix.lower() in ENDF_SUFFIXES):
                target = target_from_endf_filename(endf_file)
                if target:
                    yield evaluation, projectile, target, endf_file


def run_dece(dece: str, args: list[str]) -> list[str]:
    completed = subprocess.run(
        [dece, *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.splitlines()


def mf_mt_numbers(lines: list[str], mf: int) -> set[int]:
    numbers: set[int] = set()
    in_mf = False
    for line in lines:
        match = re.match(r"^MF\s+(\d+)", line)
        if match:
            current_mf = int(match.group(1))
            if in_mf and current_mf != mf:
                break
            in_mf = current_mf == mf
            continue
        if in_mf:
            numbers.update(int(item) for item in re.findall(r"\d+", line))
    return numbers


def mf_mt_numbers_from_endf(endf_file: Path, mf: int) -> set[int]:
    numbers: set[int] = set()
    with endf_file.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if len(line) < 75:
                continue
            try:
                line_mf = int(line[70:72])
                line_mt = int(line[72:75])
            except ValueError:
                continue
            if line_mf == mf and line_mt > 0:
                numbers.add(line_mt)
    return numbers


def scan_mf6_mts(dece: str, endf_file: Path, requested_mts: set[int] | None) -> list[int]:
    try:
        lines = run_dece(dece, ["-s", str(endf_file)])
        mf3_mts = mf_mt_numbers(lines, 3)
        mf6_mts = mf_mt_numbers(lines, 6)
    except RuntimeError as exc:
        LOGGER.warning("DeCE summary failed for %s; falling back to ENDF MF/MT scan: %s", endf_file, exc)
        mf3_mts = mf_mt_numbers_from_endf(endf_file, 3)
        mf6_mts = mf_mt_numbers_from_endf(endf_file, 6)

    candidates = requested_mts or (mf6_mts & set(DEFAULT_MF6_MTS))
    return sorted(mt for mt in candidates if mt in mf6_mts and (not mf3_mts or mt in mf3_mts))


def parse_angles(angles_arg: str | None, angle_step: float) -> tuple[float, ...]:
    if angles_arg:
        angles = sorted({float(item.strip()) for item in angles_arg.split(",") if item.strip()})
    else:
        if angle_step <= 0:
            raise ValueError("--angle-step must be positive")
        count = int(math.floor(180.0 / angle_step))
        angles = [round(i * angle_step, 8) for i in range(count + 1)]
        if not math.isclose(angles[-1], 180.0):
            angles.append(180.0)

    invalid = [angle for angle in angles if angle < 0.0 or angle > 180.0]
    if invalid:
        raise ValueError(f"angles must be between 0 and 180 degrees: {invalid}")
    return tuple(angles)


def _float_values(line: str) -> list[float] | None:
    parts = line.split()
    if len(parts) < 2:
        return None
    try:
        return [float(part) for part in parts]
    except ValueError:
        return None


def _legendre_values(mu: float, order: int) -> list[float]:
    values = [1.0]
    if order == 0:
        return values

    values.append(mu)
    for ell in range(2, order + 1):
        values.append(((2 * ell - 1) * mu * values[ell - 1] - (ell - 1) * values[ell - 2]) / ell)
    return values


def _evaluate_legendre(coefficients: list[float], angle_deg: float) -> float:
    mu = math.cos(math.radians(angle_deg))
    polynomials = _legendre_values(mu, len(coefficients) - 1)
    value = sum(
        (ell + 0.5) * coefficient * polynomials[ell]
        for ell, coefficient in enumerate(coefficients)
    )
    return value * 1e6 / (2 * math.pi)


def _mf6_header_mode(line: str) -> tuple[str | None, int]:
    if not re.search(r"#\s+Energy\s+", line):
        return None, 0

    if "PL(" in line:
        return "legendre", 0
    if "Spectrum" in line:
        return "spectrum", 1
    return None, 0


def parse_mf6_output(lines: list[str], mt: int, angles: tuple[float, ...]) -> pd.DataFrame:
    rows = []
    zap = None
    en_inc = None
    nd = 0
    lct = None
    frame = None
    collect = False
    collect_mode = None
    skip_numeric = 0

    for line in lines:
        items = line.split()

        if line.startswith("#          LCT") and len(items) >= 3:
            lct = int(items[2])
            frame = {1: "LAB", 2: "CMS", 3: "CMS/LAB", 4: "CMS/LAB"}.get(lct)
            collect = False
            continue

        if line.startswith("#          ZAP") and len(items) >= 3:
            zap = int(float(items[2]))
            collect = False
            continue

        if line.startswith("#           E1") and len(items) >= 3:
            en_inc = float(items[2]) * 1e-6
            collect = False
            continue

        if line.startswith("#           ND") and len(items) >= 3:
            nd = int(items[2])
            continue

        if line.startswith("# Energy"):
            is_discrete = "Production" in line
            collect_mode, _ = _mf6_header_mode(line)
            collect = bool(collect_mode and zap in LIGHT_PARTICLES and en_inc is not None)
            skip_numeric = nd if is_discrete else 0
            continue

        values = _float_values(line)
        if values is None:
            collect = False
            collect_mode = None
            continue

        if skip_numeric:
            skip_numeric -= 1
            continue

        if collect and zap in LIGHT_PARTICLES and en_inc is not None:
            en_out = values[0] * 1e-6
            base_row = {
                "en_inc": en_inc,
                "en_out": en_out,
                "zap": zap,
                "outgoing_particle": LIGHT_PARTICLES[zap],
                "frame": frame,
                "mt": mt,
            }

            if collect_mode == "legendre":
                if len(values) < 2:
                    continue
                coefficients = values[1:]
                for angle in angles:
                    rows.append(
                        {**base_row, "angle": angle, "data": _evaluate_legendre(coefficients, angle)}
                    )
            elif collect_mode == "spectrum" and len(values) >= 2:
                rows.append({**base_row, "angle": None, "data": values[1]})

    return pd.DataFrame(rows)


def existing_reaction_id(conn, evaluation: str, projectile: str, target: str, mt: int, outgoing: str):
    stmt = select(endf_reactions.c.reaction_id).where(
        and_(
            endf_reactions.c.evaluation == evaluation,
            endf_reactions.c.projectile == projectile,
            endf_reactions.c.target == target,
            endf_reactions.c.obs_type == DDX_OBS_TYPE,
            endf_reactions.c.mt == mt,
            endf_reactions.c.residual == outgoing,
        )
    )
    return conn.execute(stmt).scalar_one_or_none()


def insert_reaction(conn, evaluation: str, projectile: str, target: str, mt: int, outgoing: str) -> int:
    row = {
        "evaluation": evaluation,
        "obs_type": DDX_OBS_TYPE,
        "target": target,
        "projectile": projectile,
        "process": mt_to_process(projectile, DDX_OBS_TYPE, mt),
        "residual": outgoing,
        "en_inc": 0.0,
        "points": 0,
        "mf": 6,
        "mt": mt,
    }
    return conn.execute(insert(endf_reactions).values(row)).lastrowid


def replace_existing(conn, reaction_id: int) -> None:
    conn.execute(endf_ddx_data.delete().where(endf_ddx_data.c.reaction_id == reaction_id))


def load_one_file(
    conn,
    dece: str,
    evaluation: str,
    projectile: str,
    target: str,
    endf_file: Path,
    requested_mts: set[int] | None,
    angles: tuple[float, ...],
    replace: bool,
    dry_run: bool,
) -> int:
    loaded = 0
    for mt in scan_mf6_mts(dece, endf_file, requested_mts):
        try:
            lines = run_dece(dece, ["-f6", "-t", str(mt), str(endf_file)])
        except RuntimeError as exc:
            LOGGER.warning("DeCE failed for %s MT%s: %s", endf_file, mt, exc)
            continue

        df = parse_mf6_output(lines, mt, angles)
        if df.empty:
            continue

        for outgoing, out_df in df.groupby("outgoing_particle", sort=True):
            data_df = out_df.drop(columns=["mt"]).reset_index(drop=True)
            if dry_run:
                LOGGER.info(
                    "%s %s %s MT%s %s: %s points",
                    evaluation,
                    projectile,
                    target,
                    mt,
                    outgoing,
                    len(data_df),
                )
                loaded += len(data_df)
                continue

            reaction_id = existing_reaction_id(conn, evaluation, projectile, target, mt, outgoing)
            if reaction_id and replace:
                replace_existing(conn, reaction_id)
            elif reaction_id:
                LOGGER.info(
                    "skip existing %s %s %s MT%s %s",
                    evaluation,
                    projectile,
                    target,
                    mt,
                    outgoing,
                )
                continue
            else:
                reaction_id = insert_reaction(conn, evaluation, projectile, target, mt, outgoing)

            data_df["reaction_id"] = reaction_id
            data_df.to_sql(
                "endf_ddx_data",
                conn,
                index=False,
                if_exists="append",
                method="multi",
                chunksize=1000,
            )
            conn.execute(
                update(endf_reactions)
                .where(endf_reactions.c.reaction_id == reaction_id)
                .values(points=len(data_df.index))
            )
            loaded += len(data_df.index)

    return loaded


def ensure_ddx_schema(conn) -> None:
    endf_reactions.create(conn, checkfirst=True)
    endf_ddx_data.create(conn, checkfirst=True)
    columns = {row[1] for row in conn.execute(text("PRAGMA table_info(endf_ddx_data)"))}
    if "angle" not in columns:
        LOGGER.info("adding missing endf_ddx_data.angle column")
        conn.execute(text("ALTER TABLE endf_ddx_data ADD COLUMN angle FLOAT"))


def create_indexes(conn) -> None:
    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_endf_reactions_ddx "
            "ON endf_reactions(obs_type, evaluation, projectile, target, mt, residual)"
        )
    )
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_endf_ddx_data_reaction ON endf_ddx_data(reaction_id)"))
    conn.execute(
        text("CREATE INDEX IF NOT EXISTS idx_endf_ddx_data_axes ON endf_ddx_data(en_inc, angle, en_out)")
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    libraries = set(args.library or [])
    projectiles = set(args.projectile or [])
    targets = set(args.target or [])
    requested_mts = set(args.mt) if args.mt else None
    angles = parse_angles(args.angles, args.angle_step)

    total = 0
    counts: dict[tuple[str, str], int] = {}
    if args.dry_run:
        for evaluation, projectile, target, endf_file in iter_endf_files(args.archive_path, libraries, projectiles):
            key = (evaluation, projectile)
            if args.limit is not None and counts.get(key, 0) >= args.limit:
                continue
            if targets and target not in targets:
                continue

            counts[key] = counts.get(key, 0) + 1
            LOGGER.info("processing %s %s %s", evaluation, projectile, target)
            total += load_one_file(
                None,
                args.dece,
                evaluation,
                projectile,
                target,
                endf_file,
                requested_mts,
                angles,
                args.replace,
                args.dry_run,
            )
        LOGGER.info("parsed %s DDX points", total)
        return

    with engines["endftables"].connect() as conn:
        with conn.begin():
            ensure_ddx_schema(conn)
            create_indexes(conn)

        for evaluation, projectile, target, endf_file in iter_endf_files(args.archive_path, libraries, projectiles):
            key = (evaluation, projectile)
            if args.limit is not None and counts.get(key, 0) >= args.limit:
                continue
            if targets and target not in targets:
                continue

            counts[key] = counts.get(key, 0) + 1
            LOGGER.info("processing %s %s %s", evaluation, projectile, target)
            with conn.begin():
                loaded = load_one_file(
                    conn,
                    args.dece,
                    evaluation,
                    projectile,
                    target,
                    endf_file,
                    requested_mts,
                    angles,
                    args.replace,
                    args.dry_run,
                )
            total += loaded
            LOGGER.info("committed %s %s %s: %s DDX points", evaluation, projectile, target, loaded)

    LOGGER.info("loaded %s DDX points", total)


if __name__ == "__main__":
    main()
