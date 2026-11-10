"""Своя реализация MapReduce поверх объекта в RustFS.

Схема как в Hadoop:
  split   — объект делится на диапазоны байт [start, end); map-таск читает свой диапазон Range-запросом;
  record  — строка принадлежит сплиту, в котором лежит её первый байт: начало чужой строки
            пропускаем, хвост своей последней строки дочитываем из следующего диапазона;
  map     — job.mapper(fields, cols) выдаёт пары (key, value) для строки CSV;
  combine — свёртка значений одного ключа ещё на стороне map (если у задачи есть combiner);
  shuffle — ключи раскладываются по R партициям по crc32, партиция — файл на локальном диске;
  reduce  — reduce-таск r читает партицию r всех map-тасков и вызывает job.reducer(key, values).

Предполагается, что внутри полей CSV нет переводов строки (в датасете их нет: строк ровно
записей + 1). Проверка — число записей сходится с DuckDB, а строк с неверным числом полей 0.
"""

import csv
import io
import multiprocessing
import os
import pickle
import shutil
import time
import uuid
import zlib
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from itertools import groupby
from operator import itemgetter

from hw import s3, settings

TAIL_STEP = 64 * 1024  # шаг дочитывания хвоста последней строки сплита
MAX_ATTEMPTS = 3  # сколько раз запускать таск, прежде чем считать задачу проваленной
HEADER_PROBE = 64 * 1024


@dataclass(frozen=True)
class Job:
    name: str
    mapper: Callable[[list[str], dict[str, int]], Iterable[tuple]]
    reducer: Callable[[tuple, list], object]
    combiner: Callable | None = None  # ассоциативная свёртка двух значений, например operator.add
    # chunk -> строки-кандидаты; строка вне кандидатов точно ничего не даст map, её можно не разбирать
    prefilter: Callable[[bytearray], Iterable[bytes]] | None = None


@dataclass(frozen=True)
class Split:
    index: int
    start: int
    end: int


@dataclass(frozen=True)
class Failure:
    """Внедрённый отказ для проверки: map-таск сплита split падает на первой попытке.

    mode="error" — исключение внутри таска; mode="crash" — процесс-воркер умирает (os._exit),
    как при падении узла. В однопроцессном режиме crash превращается в error.
    """

    split: int
    mode: str = "error"


@dataclass(frozen=True)
class MapTask:
    job: Job
    key: str
    split: Split
    size: int
    cols: dict[str, int]
    reducers: int
    spill_dir: str
    use_combiner: bool
    use_prefilter: bool
    failure: Failure | None = None


@dataclass
class RunResult:
    results: dict
    map_stats: list[dict]
    timings: dict[str, float]
    splits: int
    retries: list[str] = field(default_factory=list)  # что падало и что перезапускалось


def make_splits(size: int, split_size: int) -> list[Split]:
    return [Split(i, s, min(s + split_size, size)) for i, s in enumerate(range(0, size, split_size))]


def read_header(key: str) -> dict[str, int]:
    probe = s3.get_range(key, 0, HEADER_PROBE - 1)
    nl = probe.find(b"\n")
    if nl == -1 and len(probe) == HEADER_PROBE:
        raise ValueError(f"заголовок CSV {key} длиннее {HEADER_PROBE} байт")
    line = probe if nl == -1 else probe[:nl]  # файл из одного заголовка без перевода строки
    header = next(csv.reader([line.decode("utf-8")]))
    return {name: i for i, name in enumerate(header)}


def read_split(key: str, split: Split, size: int) -> tuple[bytearray, int, int]:
    """Строки, принадлежащие сплиту, одним буфером; плюс число GET-запросов и прочитанных байт.

    Буфер не копируется: начало чужой строки отрезается на месте, хвост дописывается в конец.
    """
    lo = split.start - 1 if split.start > 0 else 0  # байт перед сплитом: был ли он концом строки
    buf = s3.get_range(key, lo, split.end - 1)
    requests, nbytes = 1, len(buf)
    if split.start > 0:
        nl = buf.find(b"\n")
        if nl == -1:  # весь диапазон — середина чужой строки
            return bytearray(), requests, nbytes
        del buf[: nl + 1]
    if not buf or buf.endswith(b"\n") or split.end >= size:
        return buf, requests, nbytes
    pos = split.end
    while pos < size:
        piece = s3.get_range(key, pos, min(pos + TAIL_STEP, size) - 1)
        requests, nbytes = requests + 1, nbytes + len(piece)
        nl = piece.find(b"\n")
        if nl != -1:
            buf += piece[: nl + 1]
            break
        buf += piece
        pos += len(piece)
    return buf, requests, nbytes


def count_records(body: bytearray) -> int:
    return body.count(b"\n") + (1 if body and not body.endswith(b"\n") else 0)


def partition(key: tuple, reducers: int) -> int:
    """Стабильный хеш: встроенный hash() для строк в каждом процессе свой."""
    return zlib.crc32("\x1f".join(key).encode("utf-8")) % reducers


def _owned_body(task: MapTask) -> tuple[bytearray, int, int]:
    body, requests, nbytes = read_split(task.key, task.split, task.size)
    if task.split.index == 0:  # первая строка объекта — заголовок
        nl = body.find(b"\n")
        del body[: nl + 1 if nl != -1 else len(body)]
    return body, requests, nbytes


def _inject_failure(task: MapTask, attempt: int) -> None:
    f = task.failure
    if f is None or f.split != task.split.index or attempt != 1:
        return
    if f.mode == "crash" and multiprocessing.parent_process() is not None:
        os._exit(17)  # воркер умирает без исключения, как при падении узла
    raise RuntimeError(f"внедрённый отказ маппера: сплит {f.split}, попытка {attempt}")


def _write_spill(path: str, part: dict) -> None:
    """Атомарно: при повторной попытке таска файл просто перезаписывается, недописанных спилов не бывает."""
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "wb") as f:
        pickle.dump(part, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def run_map_task(task: MapTask, attempt: int = 1) -> dict:
    _inject_failure(task, attempt)
    t0 = time.perf_counter()
    job, ncols = task.job, len(task.cols)
    body, requests, nbytes = _owned_body(task)
    t_read = time.perf_counter() - t0
    records = count_records(body)
    if task.use_prefilter and job.prefilter is not None:
        lines = job.prefilter(body)
    else:
        lines = (line for line in body.split(b"\n") if line)

    combine = job.combiner if task.use_combiner else None
    acc: dict = {} if combine else defaultdict(list)
    parsed = malformed = emitted = 0
    for fields in csv.reader(line.decode("utf-8") for line in lines):
        parsed += 1
        if len(fields) != ncols:
            malformed += 1
            continue
        for key, value in job.mapper(fields, task.cols):
            emitted += 1
            if combine is None:
                acc[key].append(value)
            elif key in acc:
                acc[key] = combine(acc[key], value)
            else:
                acc[key] = value

    parts = [{} for _ in range(task.reducers)]
    for key, value in acc.items():
        parts[partition(key, task.reducers)][key] = value
    for r, part in enumerate(parts):
        _write_spill(os.path.join(task.spill_dir, f"map-{task.split.index:04d}-part-{r:02d}.pkl"), part)
    return {
        "split": task.split.index,
        "records": records,
        "parsed": parsed,
        "malformed": malformed,
        "emitted": emitted,
        "keys": len(acc),
        "bytes": nbytes,
        "requests": requests,
        "read_s": t_read,
        "total_s": time.perf_counter() - t0,
        "attempt": attempt,
    }


def run_reduce_task(args: tuple[Job, str, int, bool], attempt: int = 1) -> dict:
    job, spill_dir, r, combined = args
    groups: dict = defaultdict(list)
    suffix = f"-part-{r:02d}.pkl"
    for name in sorted(n for n in os.listdir(spill_dir) if n.endswith(suffix)):
        with open(os.path.join(spill_dir, name), "rb") as f:
            part = pickle.load(f)
        for key, value in part.items():
            if combined:
                groups[key].append(value)  # частичный итог одного map-таска
            else:
                groups[key].extend(value)  # все сырые значения
    return {key: job.reducer(key, values) for key, values in groups.items()}


class _Workers:
    """Пул процессов на spawn, который пересоздаётся, если какой-то воркер умер."""

    def __init__(self, workers: int):
        self.workers, self.pool = workers, None

    def get(self) -> ProcessPoolExecutor:
        if self.pool is None:
            # spawn, а не fork (по умолчанию на Linux): с fork воркеры унаследовали бы клиент boto3
            # родителя вместе с открытым соединением, и ответы на Range-запросы перепутались бы.
            spawn = multiprocessing.get_context("spawn")
            self.pool = ProcessPoolExecutor(max_workers=self.workers, mp_context=spawn)
        return self.pool

    def reset(self) -> None:
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)
            self.pool = None


def _run_with_retries(fn: Callable, tasks: list, workers: _Workers | None, label: str, events: list[str]) -> list:
    """Выполняет таски и перезапускает упавшие, как Hadoop: до MAX_ATTEMPTS попыток на таск.

    Таск падает либо с исключением, либо вместе с процессом-воркером (BrokenProcessPool: тогда
    перезапускаются все незавершённые таски этого пула, а сам пул создаётся заново).
    """
    results: list = [None] * len(tasks)
    attempts = [0] * len(tasks)
    pending = list(range(len(tasks)))
    while pending:
        failed: list[tuple[int, BaseException]] = []
        if workers is None:
            for i in pending:
                attempts[i] += 1
                try:
                    results[i] = fn(tasks[i], attempts[i])
                except Exception as exc:  # noqa: BLE001 — любой отказ таска ведёт к перезапуску
                    failed.append((i, exc))
        else:
            pool = workers.get()
            futures = {}
            for i in pending:
                attempts[i] += 1
                futures[pool.submit(fn, tasks[i], attempts[i])] = i
            for fut in as_completed(futures):
                try:
                    results[futures[fut]] = fut.result()
                except Exception as exc:  # noqa: BLE001
                    failed.append((futures[fut], exc))
            if any(isinstance(exc, BrokenProcessPool) for _, exc in failed):
                workers.reset()
        broken = sorted(i for i, exc in failed if isinstance(exc, BrokenProcessPool))
        if broken:  # одна причина на все таски пула — одной строкой
            events.append(f"процесс-воркер умер (BrokenProcessPool): пул создан заново, "
                          f"незавершённых {label}ов: {len(broken)}")
        for i, exc in sorted(failed, key=lambda f: f[0]):
            if not isinstance(exc, BrokenProcessPool):
                events.append(f"{label} {i}: попытка {attempts[i]} не удалась — {type(exc).__name__}: {exc}")
            if attempts[i] >= MAX_ATTEMPTS:
                raise RuntimeError(f"{label} {i} не выполнен за {MAX_ATTEMPTS} попытки") from exc
        pending = sorted(i for i, _ in failed)
        if pending:
            events.append(f"перезапуск {label}ов ({len(pending)} шт.), попытка {max(attempts[i] for i in pending) + 1}")
    return results


def run(
    job: Job,
    key: str = settings.RAW_KEY,
    *,
    split_size: int = 32 * 2**20,
    workers: int = os.cpu_count() or 4,
    reducers: int = 4,
    use_combiner: bool = True,
    use_prefilter: bool = True,
    failure: Failure | None = None,
) -> RunResult:
    t0 = time.perf_counter()
    size = s3.object_size(key)
    cols = read_header(key)
    splits = make_splits(size, split_size)
    spill_dir = settings.WORK_DIR / f"{job.name}-{uuid.uuid4().hex[:8]}"
    spill_dir.mkdir(parents=True)
    combined = use_combiner and job.combiner is not None
    map_tasks = [
        MapTask(job, key, s, size, cols, reducers, str(spill_dir), use_combiner, use_prefilter, failure)
        for s in splits
    ]
    reduce_tasks = [(job, str(spill_dir), r, combined) for r in range(reducers)]
    events: list[str] = []
    pool = None if workers == 1 else _Workers(workers)  # 1 — без пула процессов, честный однопроцессный замер
    try:
        t_map = time.perf_counter()
        map_stats = _run_with_retries(run_map_task, map_tasks, pool, "map-таск", events)
        t_reduce = time.perf_counter()
        parts = _run_with_retries(run_reduce_task, reduce_tasks, pool, "reduce-таск", events)
        t_end = time.perf_counter()
    finally:
        if pool is not None:
            pool.reset()
        shutil.rmtree(spill_dir, ignore_errors=True)
    results: dict = {}
    for part in parts:
        results.update(part)  # партиции не пересекаются по ключам
    timings = {"setup_s": t_map - t0, "map_s": t_reduce - t_map, "reduce_s": t_end - t_reduce, "total_s": t_end - t0}
    return RunResult(results, map_stats, timings, len(splits), events)


def run_baseline(job: Job, key: str = settings.RAW_KEY, *, split_size: int = 32 * 2**20) -> RunResult:
    """Наивный MapReduce — точка «до» для бонусного задания.

    Один процесс, сплиты по очереди, каждая строка разбирается целиком, map складывает все пары
    (key, value) в общий список без combiner, shuffle — сортировка списка, reduce — groupby.
    Чтение сплитов и mapper те же, что в run(), поэтому результат обязан совпасть.
    """
    t0 = time.perf_counter()
    size = s3.object_size(key)
    cols = read_header(key)
    ncols = len(cols)
    splits = make_splits(size, split_size)
    pairs, map_stats = [], []
    for split in splits:
        task = MapTask(job, key, split, size, cols, 1, "", False, False)
        body, requests, nbytes = _owned_body(task)
        records = malformed = 0
        for fields in csv.reader(io.StringIO(body.decode("utf-8"))):
            records += 1
            if len(fields) != ncols:
                malformed += 1
                continue
            pairs.extend(job.mapper(fields, cols))
        map_stats.append(
            {"split": split.index, "records": records, "malformed": malformed, "bytes": nbytes, "requests": requests}
        )
    t_shuffle = time.perf_counter()
    pairs.sort(key=itemgetter(0))
    t_reduce = time.perf_counter()
    results = {k: job.reducer(k, [v for _, v in grp]) for k, grp in groupby(pairs, key=itemgetter(0))}
    t_end = time.perf_counter()
    timings = {
        "map_s": t_shuffle - t0,
        "shuffle_s": t_reduce - t_shuffle,
        "reduce_s": t_end - t_reduce,
        "total_s": t_end - t0,
        "pairs": len(pairs),
    }
    return RunResult(results, map_stats, timings, len(splits))
