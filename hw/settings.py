"""Настройки стенда: адрес RustFS, ключи и имена объектов. Значения берутся из .env."""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]  # как docker compose: KEY="значение"
            os.environ.setdefault(key.strip(), value)


_load_dotenv(ROOT / ".env")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9100")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "hh-lake")
SOURCE_CSV = Path(os.path.expanduser(os.environ.get("SOURCE_CSV", "~/Downloads/it_vacancies_2024.csv")))

RAW_KEY = "raw/it_vacancies_2024.csv"
PARQUET_KEY = "curated/vacancies.parquet"
SEGMENT_CSV_KEY = "segments/go/it_vacancies_go.csv"
RESULTS_PREFIX = "results"

RESULTS_DIR = ROOT / "results"
WORK_DIR = ROOT / "work"
ALIASES_CSV = ROOT / "config" / "skill_aliases.csv"
SQL_DIR = ROOT / "sql"


def s3_url(key: str) -> str:
    return f"s3://{BUCKET}/{key}"
