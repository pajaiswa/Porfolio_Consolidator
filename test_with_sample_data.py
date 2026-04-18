"""
test_with_sample_data.py
========================
Temporarily swaps real personal data for sample data, runs the full pipeline,
then restores the real data.

Usage:
  uv run python test_with_sample_data.py               # test & restore real data
  uv run python test_with_sample_data.py --keep-output # leave sample output so dashboard shows sample data

Safe to run — real files are backed up to data/input/_backup/ and restored
automatically after the test, even if the pipeline fails.
"""
import shutil
import subprocess
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SAMPLE = Path("sample_data")
INPUT  = Path("data/input")
BACKUP = INPUT / "_backup"
OUTPUT = Path("data/output")

# Mapping: sample source file → real destination path
SAMPLE_FILES: dict[Path, Path] = {
    SAMPLE / "mf"     / "Rahul_MF.xlsx"               : INPUT / "mf"     / "Rahul_MF.xlsx",
    SAMPLE / "stock"  / "Rahul_stocks.xlsx"            : INPUT / "stock"  / "Rahul_stocks.xlsx",
    SAMPLE / "EPF"    / "epf_config.csv"               : INPUT / "EPF"    / "epf_config.csv",
    SAMPLE / "FD"     / "FD_details.xlsx"              : INPUT / "FD"     / "FD_details.xlsx",
    SAMPLE / "global" / "global_transactions.csv"      : INPUT / "global" / "global_transactions.csv",
}

# Real files that we need to move away so they don't get processed together
REAL_FILES_TO_BACKUP: list[Path] = [
    *sorted((INPUT / "mf").glob("*.xlsx")),
    *sorted((INPUT / "mf").glob("*.pdf")),
    *sorted((INPUT / "stock").glob("*.xlsx")),
    *[INPUT / "EPF" / "epf_config.csv"],
    *sorted((INPUT / "FD").glob("*.xlsx")),
    *[INPUT / "global" / "global_transactions.csv"],
    *sorted((INPUT / "NPS").glob("*.pdf")),
]

# Old output that we'll also back up so we can compare / restore
OUTPUT_BACKUP = OUTPUT.parent / "output_backup_real"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def backup_real_data() -> None:
    """Move real personal data files into a temporary backup directory."""
    logger.info("=== Backing up real data files ===")
    BACKUP.mkdir(parents=True, exist_ok=True)

    for src in REAL_FILES_TO_BACKUP:
        if not src.exists():
            continue
        rel = src.relative_to(INPUT)
        dst = BACKUP / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        logger.info("  Backed up: %s", rel)

    # Also back up existing output so we can inspect diffs later
    if OUTPUT.exists():
        if OUTPUT_BACKUP.exists():
            shutil.rmtree(OUTPUT_BACKUP)
        shutil.copytree(OUTPUT, OUTPUT_BACKUP)
        shutil.rmtree(OUTPUT)
        logger.info("  Backed up existing data/output → data/output_backup_real")


def install_sample_data() -> None:
    """Copy sample files into the real input directories."""
    logger.info("=== Installing sample data files ===")
    for src, dst in SAMPLE_FILES.items():
        if not src.exists():
            logger.warning("  Sample file not found, skipping: %s", src)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))
        logger.info("  Installed: %s → %s", src, dst)


def restore_real_data(keep_output: bool = False) -> None:
    """Move real personal data files back to their original locations."""
    logger.info("=== Restoring real data files ===")

    # Remove sample files we installed
    for _, dst in SAMPLE_FILES.items():
        if dst.exists():
            dst.unlink()

    # Move backed-up real files back
    for backed_up in sorted(BACKUP.rglob("*")):
        if backed_up.is_dir():
            continue
        rel = backed_up.relative_to(BACKUP)
        dst = INPUT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(backed_up), str(dst))
        logger.info("  Restored: %s", rel)

    # Clean up empty backup dir
    if BACKUP.exists():
        shutil.rmtree(BACKUP, ignore_errors=True)

    if keep_output:
        # Leave sample data/output in place — dashboard will show sample data
        if OUTPUT_BACKUP.exists():
            shutil.rmtree(OUTPUT_BACKUP)  # discard the real output backup
        logger.info("  Keeping sample data/output for dashboard preview.")
        logger.info("  ⚠  Run 'uv run python run_all.py' to restore real output.")
    else:
        # Restore original output
        if OUTPUT_BACKUP.exists():
            if OUTPUT.exists():
                shutil.rmtree(OUTPUT)
            shutil.copytree(OUTPUT_BACKUP, OUTPUT)
            shutil.rmtree(OUTPUT_BACKUP)
            logger.info("  Restored data/output from backup.")

    logger.info("=== Real data fully restored ===")


def run_pipeline() -> bool:
    """Runs run_all.py and returns True on success."""
    logger.info("=== Running full pipeline with sample data ===")
    result = subprocess.run(
        ["uv", "run", "python", "run_all.py"],
        text=True,
        encoding="utf-8",
    )
    # uv may print warnings to stderr (e.g. pydantic_core RECORD issue) which
    # don't reflect the actual pipeline outcome. We trust run_all.py's returncode.
    logger.info("Pipeline process exited with code: %d", result.returncode)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    keep_output = "--keep-output" in sys.argv

    backup_real_data()
    install_sample_data()

    pipeline_ok = False
    try:
        pipeline_ok = run_pipeline()
    finally:
        restore_real_data(keep_output=keep_output)

    if pipeline_ok:
        logger.info("✅  Pipeline ran successfully with sample data.")
        if keep_output:
            logger.info("📊  Dashboard now shows sample data. Refresh http://localhost:8501")
            logger.info("    To restore real data: uv run python run_all.py")
    else:
        logger.error("❌  Pipeline encountered errors — check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
