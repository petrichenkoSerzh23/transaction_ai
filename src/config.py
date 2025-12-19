from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_INPUT_DIR = BASE_DIR / "data" / "input"
DATA_OUTPUT_DIR = BASE_DIR / "data" / "output"

TRANSACTION_FILE = DATA_INPUT_DIR / "transaction-report.csv"
