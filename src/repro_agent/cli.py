from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from .config import load_pipeline_config
from .db.connectors import build_masked_postgres_dsn, load_mimic_pg_env, missing_required_fields
from .pipeline import PaperReproPipeline


def _resolve_project_root(path: str | None) -> Path:
    if path:
        return Path(path).resolve()
    return Path.cwd()


def _load_project_env(project_root: Path) -> None:
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        # Keep shell/session overrides if already set.
        if key and key not in os.environ:
            os.environ[key] = value


def cmd_dry_run(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(args.project_root)
    _load_project_env(project_root)
    config_path = (project_root / args.config).resolve()
    config = load_pipeline_config(config_path)

    pipeline = PaperReproPipeline(project_root=project_root, config=config)
    summary = pipeline.run(dry_run=True)
    print(json.dumps(summary.as_dict(), indent=2, ensure_ascii=False))
    return 0 if summary.status.value == "success" else 2


def cmd_run(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(args.project_root)
    _load_project_env(project_root)
    config_path = (project_root / args.config).resolve()
    config = load_pipeline_config(config_path)
    pipeline = PaperReproPipeline(project_root=project_root, config=config)
    summary = pipeline.run(dry_run=False)
    print(json.dumps(summary.as_dict(), indent=2, ensure_ascii=False))
    return 0 if summary.status.value == "success" else 2


def cmd_validate_env(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    cfg = load_mimic_pg_env()
    missing = missing_required_fields(cfg)
    print("MIMIC connection (masked):")
    print(build_masked_postgres_dsn(cfg))
    if missing:
        print("\nMissing required environment variables:")
        for item in missing:
            print(f"- {item}")
        return 1
    print("\nEnvironment looks ready for DB connection wiring.")
    return 0


def cmd_probe_db(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    cfg = load_mimic_pg_env()
    missing = missing_required_fields(cfg)
    if missing:
        print("Missing required env vars:")
        for item in missing:
            print(f"- {item}")
        return 1

    conn = (
        f"host={cfg.host} port={cfg.port} user={cfg.user} "
        f"dbname={cfg.db} sslmode={cfg.sslmode}"
    )
    query = (
        "SELECT current_database() AS db, current_user AS user_name; "
        "SELECT table_schema, COUNT(*) AS n_tables "
        "FROM information_schema.tables "
        "WHERE table_schema LIKE 'mimiciv%' "
        "GROUP BY table_schema ORDER BY table_schema;"
    )

    env = os.environ.copy()
    env["PGPASSWORD"] = cfg.password
    cmd = ["psql", conn, "-c", query]
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        print(completed.stderr.strip())
        return completed.returncode
    print(completed.stdout.strip())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="paper-repro", description="Clinical paper reproduction multi-subagent framework")
    sub = parser.add_subparsers(dest="command", required=True)

    dry = sub.add_parser("dry-run", help="Run full pipeline with synthetic/stub artifacts")
    dry.add_argument("--project-root", type=str, default=".")
    dry.add_argument("--config", type=str, default="configs/pipeline.example.yaml")
    dry.set_defaults(func=cmd_dry_run)

    run = sub.add_parser("run", help="Run pipeline in production mode (adapter implementation required)")
    run.add_argument("--project-root", type=str, default=".")
    run.add_argument("--config", type=str, default="configs/pipeline.example.yaml")
    run.set_defaults(func=cmd_run)

    env = sub.add_parser("validate-env", help="Validate required DB environment variables")
    env.add_argument("--project-root", type=str, default=".")
    env.set_defaults(func=cmd_validate_env)

    probe = sub.add_parser("probe-db", help="Probe PostgreSQL connection and visible MIMIC schemas")
    probe.add_argument("--project-root", type=str, default=".")
    probe.set_defaults(func=cmd_probe_db)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
