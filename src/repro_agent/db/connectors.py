from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class PostgresEnvConfig:
    host: str
    port: str
    db: str
    user: str
    password: str
    sslmode: str


def _read_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def load_mimic_pg_env(prefix: str = "MIMIC_PG") -> PostgresEnvConfig:
    return PostgresEnvConfig(
        host=_read_env(f"{prefix}_HOST"),
        port=_read_env(f"{prefix}_PORT", "5432"),
        db=_read_env(f"{prefix}_DB"),
        user=_read_env(f"{prefix}_USER"),
        password=_read_env(f"{prefix}_PASSWORD"),
        sslmode=_read_env(f"{prefix}_SSLMODE", "require"),
    )


def missing_required_fields(cfg: PostgresEnvConfig) -> list[str]:
    missing: list[str] = []
    if not cfg.host:
        missing.append("MIMIC_PG_HOST")
    if not cfg.db:
        missing.append("MIMIC_PG_DB")
    if not cfg.user:
        missing.append("MIMIC_PG_USER")
    if not cfg.password:
        missing.append("MIMIC_PG_PASSWORD")
    return missing


def build_masked_postgres_dsn(cfg: PostgresEnvConfig) -> str:
    hidden = "***" if cfg.password else "(empty)"
    return (
        f"postgresql://{cfg.user}:{hidden}@{cfg.host}:{cfg.port}/{cfg.db}"
        f"?sslmode={cfg.sslmode}"
    )

