"""Правила варианта Go: отбор вакансий, нормализация навыков и зарплаты.

Это единственный источник правил для MapReduce. SQL в sql/ повторяет их один к одному,
поэтому оба способа обязаны дать одинаковый результат.
"""

import csv
import re
from itertools import combinations

from hw import settings

# В названии есть отдельное слово Go или Golang: регистр не важен, границы слова — только ASCII,
# как у RE2 в DuckDB: (^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)
GO_TITLE = re.compile(r"(?<![A-Za-z0-9_])(?:go|golang)(?![A-Za-z0-9_])", re.I | re.A)

# Предфильтр сырых строк CSV: если название подходит, в байтах строки есть слово go/golang.
# Ищем по копии чанка в нижнем регистре шаблоном, который начинается с литерала: такой префикс
# движок re проверяет быстро, а левую границу слова проверяем вручную. Это эквивалент
# rb"(?<![A-Za-z0-9_])go(?:lang)?(?![A-Za-z0-9_])" с re.I, но в ~25 раз быстрее.
_GO_WORD = re.compile(rb"go(?:lang)?(?![a-z0-9_])")
# Первая версия предфильтра — оставлена для замера «до» (scripts/benchmark.py, mr_w1_regex).
_GO_WORD_LOOKBEHIND = re.compile(rb"(?<![A-Za-z0-9_])go(?:lang)?(?![A-Za-z0-9_])", re.I)
_WORD_BYTES = frozenset(b"abcdefghijklmnopqrstuvwxyz0123456789_")

# Обычные пробелы, табуляции, неразрывный пробел, zero-width space и BOM по краям навыка.
STRIP_CHARS = " \t\n\r\f\v\u00a0\u200b\ufeff"

NET_FACTOR = 0.87  # НДФЛ 13 % в 2024 году: «до вычета налогов» -> «на руки»
SALARY_MIN, SALARY_MAX = 10_000, 1_500_000  # руб./мес. на руки, всё вне диапазона — выбросы
# Поля, совпадение которых означает одно и то же предложение (копии вакансии по городам).
OFFER_FIELDS = ("employer_id", "name", "key_skills", "salary_from", "salary_to", "currency", "gross")


def _load_aliases() -> dict[str, str]:
    with open(settings.ALIASES_CSV, encoding="utf-8", newline="") as f:
        return {row["alias"]: row["canonical"] for row in csv.DictReader(f)}


ALIASES = _load_aliases()


def go_candidate_lines(body: bytes | bytearray):
    """Строки чанка, где встречается слово go или golang, — только их стоит разбирать как CSV."""
    low = body.lower()  # меняются только ASCII A–Z, смещения байт те же
    pos = 0
    while (m := _GO_WORD.search(low, pos)) is not None:
        start = m.start()
        if start and low[start - 1] in _WORD_BYTES:  # часть другого слова: mongo, django, algorithm
            pos = m.end()
            continue
        line_start = body.rfind(b"\n", 0, start) + 1
        line_end = body.find(b"\n", m.end())
        line_end = len(body) if line_end == -1 else line_end
        yield body[line_start:line_end]
        pos = line_end + 1


def go_candidate_lines_regex(body: bytes | bytearray):
    """То же самое одной регуляркой с lookbehind и re.I — медленно: нет быстрого поиска по литералу."""
    pos = 0
    while (m := _GO_WORD_LOOKBEHIND.search(body, pos)) is not None:
        line_start = body.rfind(b"\n", 0, m.start()) + 1
        line_end = body.find(b"\n", m.end())
        line_end = len(body) if line_end == -1 else line_end
        yield body[line_start:line_end]
        pos = line_end + 1


def is_go_title(title: str) -> bool:
    return GO_TITLE.search(title) is not None


def normalize_skill(raw: str) -> str:
    # lower() по одной букве, как в DuckDB: str.lower() целой строки учитывает контекст
    # (конечная сигма) и превращает İ в два символа
    skill = "".join(ch.lower()[0] for ch in raw.strip(STRIP_CHARS))
    return ALIASES.get(skill, skill)


def vacancy_skills(field: str) -> list[str]:
    """Навыки вакансии без пустых и повторов, по алфавиту — комбинации получаются каноничными."""
    if not field:
        return []
    return sorted({s for s in map(normalize_skill, field.split(";")) if s})


def skill_combinations(skills: list[str], k_min: int = 1, k_max: int = 3):
    """Все комбинации из k навыков; k = 0 даёт пустой кортеж — «все вакансии»."""
    for k in range(k_min, k_max + 1):
        yield from combinations(skills, k)


def salary_net_rub(salary_from: str, salary_to: str, currency: str, gross: str) -> float | None:
    """Зарплата одной точкой, руб./мес. на руки; None — если не в рублях, нет вилки или выброс."""
    if currency != "RUR":
        return None
    lo = float(salary_from) if salary_from else None
    hi = float(salary_to) if salary_to else None
    if lo is None and hi is None:
        return None
    point = (lo + hi) / 2 if lo is not None and hi is not None else (lo if lo is not None else hi)
    point *= NET_FACTOR if gross == "true" else 1.0
    return point if SALARY_MIN <= point <= SALARY_MAX else None
