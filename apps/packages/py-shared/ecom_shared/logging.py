"""
ecom_shared/logging.py — Centralised logging configuration for all Python services.

Usage (in any service, call once at startup):
    from ecom_shared.logging import setup_logging
    setup_logging()           # reads LOG_LEVEL / LOG_FORMAT from env

Usage (in any module, just use stdlib logging as normal):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Something happened", extra={"order_id": 42})

Formats
-------
- ``"json"``  (default in production) — one JSON object per line, ready for
  Loki / Datadog / CloudWatch.
- ``"pretty"`` (default when LOG_ENV=local) — coloured, human-readable output
  with module path and line number.

Environment variables
---------------------
LOG_LEVEL   DEBUG | INFO | WARNING | ERROR | CRITICAL  (default: INFO)
LOG_FORMAT  json | pretty                              (default: json)
LOG_ENV     local | production                         (default: production)
            When set to "local", LOG_FORMAT defaults to "pretty".
SERVICE_NAME  Embedded in every JSON log line.         (default: "ecom-service")
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = ["setup_logging", "get_logger"]


def get_logger(name: str) -> logging.Logger:
    """
    Return a standard ``logging.Logger`` for *name*.

    This is a thin convenience wrapper so callers can do::

        from ecom_shared.logging import get_logger
        logger = get_logger(__name__)

    instead of importing ``logging`` directly.  Both approaches work fine
    once ``setup_logging()`` has been called.
    """
    return logging.getLogger(name)


def setup_logging(
    level: str | None = None,
    fmt: str | None = None,
    service_name: str | None = None,
) -> None:
    """
    Configure the root logger for the calling service.

    Call this **once**, as early as possible in your application entry-point
    (e.g. ``main.py`` or the FastAPI ``lifespan`` startup hook).

    Parameters
    ----------
    level:
        Override log level (``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``,
        ``CRITICAL``).  Falls back to ``LOG_LEVEL`` env-var, then ``INFO``.
    fmt:
        Override format: ``"json"`` or ``"pretty"``.  Falls back to
        ``LOG_FORMAT`` env-var, then auto-detected from ``LOG_ENV``.
    service_name:
        Label embedded in JSON logs.  Falls back to ``SERVICE_NAME`` env-var,
        then ``"ecom-service"``.
    """
    _level = _resolve_level(level)
    _fmt = _resolve_format(fmt)
    _svc = service_name or os.getenv("SERVICE_NAME", "ecom-service")

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(_level)

    if _fmt == "json":
        handler.setFormatter(_JsonFormatter(service_name=_svc))
    else:
        handler.setFormatter(_PrettyFormatter())

    root = logging.getLogger()
    root.setLevel(_level)

    # Remove any handlers attached by libraries before us (e.g. uvicorn)
    root.handlers.clear()
    root.addHandler(handler)

    # Silence noisy third-party loggers
    for noisy in ("httpx", "httpcore", "urllib3", "botocore", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger(__name__).debug(
        "Logging initialised: level=%s format=%s service=%s",
        logging.getLevelName(_level),
        _fmt,
        _svc,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_level(override: str | None) -> int:
    raw = override or os.getenv("LOG_LEVEL", "INFO")
    level = getattr(logging, raw.upper(), None)
    if not isinstance(level, int):
        level = logging.INFO
    return level


def _resolve_format(override: str | None) -> str:
    if override:
        return override.lower()
    env_fmt = os.getenv("LOG_FORMAT", "").lower()
    if env_fmt in ("json", "pretty"):
        return env_fmt
    # Auto-detect: local dev → pretty, everything else → json
    return "pretty" if os.getenv("LOG_ENV", "production").lower() == "local" else "json"


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

_LEVEL_COLORS = {
    "DEBUG": "\033[36m",     # Cyan
    "INFO": "\033[32m",      # Green
    "WARNING": "\033[33m",   # Yellow
    "ERROR": "\033[31m",     # Red
    "CRITICAL": "\033[35m",  # Magenta
}
_RESET = "\033[0m"
_DIM = "\033[2m"
_BOLD = "\033[1m"


class _PrettyFormatter(logging.Formatter):
    """Coloured, human-readable formatter for local development."""

    _FMT = (
        "{dim}{asctime}{reset} "
        "{color}{bold}{levelname:<8}{reset} "
        "{dim}{name}:{lineno}{reset}  "
        "{message}"
    )

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        color = _LEVEL_COLORS.get(record.levelname, "")
        parts = self._FMT.format(
            dim=_DIM,
            bold=_BOLD,
            color=color,
            reset=_RESET,
            asctime=self.formatTime(record, "%H:%M:%S"),
            levelname=record.levelname,
            name=record.name,
            lineno=record.lineno,
            message=record.getMessage(),
        )
        if record.exc_info:
            parts += "\n" + self.formatException(record.exc_info)
        return parts


class _JsonFormatter(logging.Formatter):
    """
    Structured JSON formatter — one JSON object per log line.

    Output fields:
        timestamp, level, service, logger, message, module, lineno
        + any ``extra`` fields passed to the log call
        + exc_info (if an exception is attached)
    """

    def __init__(self, service_name: str = "ecom-service") -> None:
        super().__init__()
        self._service = service_name

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self._service,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "lineno": record.lineno,
        }

        # Attach any ``extra={...}`` fields the caller passed in
        _STDLIB_ATTRS = {
            "args", "asctime", "created", "exc_info", "exc_text", "filename",
            "funcName", "levelname", "levelno", "lineno", "message", "module",
            "msecs", "msg", "name", "pathname", "process", "processName",
            "relativeCreated", "stack_info", "thread", "threadName",
        }
        for key, value in record.__dict__.items():
            if key not in _STDLIB_ATTRS and not key.startswith("_"):
                try:
                    json.dumps(value)  # quick serializability check
                    payload[key] = value
                except (TypeError, ValueError):
                    payload[key] = str(value)

        if record.exc_info:
            payload["exc_info"] = "".join(traceback.format_exception(*record.exc_info))

        return json.dumps(payload, ensure_ascii=False)
