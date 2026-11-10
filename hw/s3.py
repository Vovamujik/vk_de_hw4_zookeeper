"""Клиент S3 для RustFS и чтение диапазона байт объекта."""

import os

import boto3
from botocore.config import Config

from hw import settings

_client = None


def _forget_client() -> None:
    global _client
    _client = None


# Клиент и его соединения не должны переходить в дочерний процесс через fork.
os.register_at_fork(after_in_child=_forget_client)


def client():
    """Один клиент на процесс: boto3-клиенты нельзя передавать между процессами."""
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT,
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            region_name=settings.S3_REGION,
            config=Config(
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5, "mode": "standard"},
                max_pool_connections=32,
            ),
        )
    return _client


def object_size(key: str) -> int:
    return client().head_object(Bucket=settings.BUCKET, Key=key)["ContentLength"]


def get_range(key: str, start: int, end: int) -> bytearray:
    """Байты [start, end] включительно — один HTTP GET с заголовком Range.

    Ответ читается кусками по 1 МиБ сразу в заранее выделенный буфер: без промежуточной
    склейки, которая на время держала бы в памяти вторую копию диапазона.
    """
    body = client().get_object(Bucket=settings.BUCKET, Key=key, Range=f"bytes={start}-{end}")["Body"]
    buf = bytearray(end - start + 1)
    view, pos = memoryview(buf), 0
    for chunk in body.iter_chunks(2**20):
        view[pos : pos + len(chunk)] = chunk
        pos += len(chunk)
    view.release()
    del buf[pos:]  # объект кончился раньше запрошенного конца
    return buf


def upload_dir(local_dir, prefix: str) -> list[str]:
    """Кладёт все файлы папки в RustFS под prefix/; возвращает ключи объектов."""
    keys = []
    for path in sorted(local_dir.iterdir()):
        if path.is_file():
            key = f"{prefix}/{path.name}"
            client().upload_file(str(path), settings.BUCKET, key)
            keys.append(key)
    return keys


def read_text(key: str) -> str:
    return client().get_object(Bucket=settings.BUCKET, Key=key)["Body"].read().decode("utf-8")
