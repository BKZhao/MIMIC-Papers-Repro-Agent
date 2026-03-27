from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from .agentic.runner import AgentRunner
from .config import PipelineConfig, load_pipeline_config
from .contracts import SessionState, TaskContract
from .dataset_adapters import get_dataset_adapter
from .db.connectors import build_masked_postgres_dsn, load_mimic_pg_env, missing_required_fields
from .llm import LLMError, OpenAICompatibleClient
from .openclaw_bridge import (
    continue_session as bridge_continue_session,
    describe_openclaw_integration,
    get_lobster_request_template as bridge_get_lobster_request_template,
    handle_lobster_request as bridge_handle_lobster_request,
    run_preset_pipeline as bridge_run_preset_pipeline,
)
from .pipeline import PaperReproPipeline
from .paper.presets import get_paper_preset
from .runtime import LocalRuntime
from .registry.skill_contracts import load_skill_contract_manifest
from .paper.templates import infer_study_template
from .paper.builder import (
    apply_follow_up_answers,
    build_task_contract,
    find_missing_high_impact_fields,
    normalize_task_contract,
    summarize_task_contract,
)


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


def cmd_run_preset_pipeline(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(args.project_root)
    _load_project_env(project_root)
    payload = bridge_run_preset_pipeline(
        project_root=project_root,
        config_path=(project_root / args.config).resolve(),
        dry_run=_resolve_dry_run_override(args),
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0 if str(payload.get("status", "")) == "success" else 2


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


def cmd_probe_llm(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    config = load_pipeline_config((project_root / args.config).resolve())
    client = OpenAICompatibleClient(config.llm)

    payload = {
        "provider": config.llm.provider,
        "base_url": config.llm.base_url,
        "model": args.model or config.llm.default_model,
        "api_key_env": config.llm.api_key_env,
    }
    if not client.is_enabled():
        payload["status"] = "not_configured"
        payload["error"] = f"Missing API key in env var {config.llm.api_key_env}"
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 1

    messages = [
        {
            "role": "system",
            "content": (
                "You are validating an OpenAI-compatible endpoint for a clinical paper reproduction "
                "agent. Return only JSON with keys status, message, and mode."
            ),
        },
        {
            "role": "user",
            "content": args.prompt,
        },
    ]
    try:
        response_payload, response = client.complete_json(
            messages,
            model=(args.model or None),
            temperature=0.0,
            max_tokens=args.max_tokens,
        )
    except LLMError as exc:
        payload["status"] = "error"
        payload["error"] = str(exc)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 2

    payload["status"] = "success"
    payload["resolved_model"] = response.model
    payload["response"] = response_payload
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def cmd_extract_analysis_dataset(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    script_path = project_root / "scripts" / "profiles" / "build_profile_analysis_dataset.py"
    cmd = [
        "python3",
        str(script_path),
        "--project-root",
        str(project_root),
        "--profile",
        args.profile,
        "--admit-year-start",
        str(args.admit_year_start) if args.admit_year_start is not None else "",
        "--admit-year-end",
        str(args.admit_year_end) if args.admit_year_end is not None else "",
        "--output",
        args.output,
        "--missingness-output",
        args.missingness_output,
        "--sepsis-source",
        args.sepsis_source,
    ]
    cmd = [item for item in cmd if item != ""]
    completed = subprocess.run(cmd, text=True, capture_output=True)
    if completed.stdout.strip():
        print(completed.stdout.strip())
    if completed.returncode != 0:
        if completed.stderr.strip():
            print(completed.stderr.strip())
        return completed.returncode
    return 0


def cmd_describe_openclaw(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    payload = describe_openclaw_integration(project_root)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def cmd_describe_skills(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    payload = load_skill_contract_manifest(project_root).as_dict()
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def cmd_plan_task(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    config = load_pipeline_config((project_root / args.config).resolve())

    contract, session, payload = _plan_task_flow(
        project_root=project_root,
        config=config,
        paper_path=args.paper_path,
        instructions=_read_instructions(args, project_root),
        session_id=args.session_id,
        use_llm=not args.no_llm,
        interactive=False,
    )
    print(
        json.dumps(
            {
                "session_id": session.session_id,
                "task_contract_path": session.task_contract_path,
                "missing_high_impact_fields": payload["missing_high_impact_fields"],
                "used_llm": payload["used_llm"],
                "llm_error": payload["llm_error"],
                "execution_backend": payload["execution_backend"],
                "execution_supported": payload["execution_supported"],
                "agent_decision": payload["agent_decision"],
                "follow_up_questions": payload["follow_up_questions"],
                "recommended_run_profile": payload["recommended_run_profile"],
                "selected_agent_sequence": payload["selected_agent_sequence"],
                "agent_reply": payload["agent_reply"],
                "analysis_family_route": payload["analysis_family_route"],
                "analysis_family_route_path": payload["analysis_family_route_path"],
                "paper_evidence": payload["paper_evidence"],
                "paper_evidence_path": payload["paper_evidence_path"],
                "paper_spec_surface": payload["paper_spec_surface"],
                "paper_spec_surface_path": payload["paper_spec_surface_path"],
                "analysis_spec_surface": payload["analysis_spec_surface"],
                "analysis_spec_surface_path": payload["analysis_spec_surface_path"],
                "task_build_mode": payload["task_build_mode"],
                "preset": payload["preset"],
                "study_template": payload["study_template"],
                "task_summary": summarize_task_contract(contract),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


def cmd_chat(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    config = load_pipeline_config((project_root / args.config).resolve())

    contract, session, payload = _plan_task_flow(
        project_root=project_root,
        config=config,
        paper_path=args.paper_path,
        instructions=_read_instructions(args, project_root),
        session_id=args.session_id,
        use_llm=not args.no_llm,
        interactive=not args.no_prompt,
    )
    response: dict[str, object] = {
        "session_id": session.session_id,
        "task_contract_path": session.task_contract_path,
        "missing_high_impact_fields": payload["missing_high_impact_fields"],
        "used_llm": payload["used_llm"],
        "llm_error": payload["llm_error"],
        "execution_backend": payload["execution_backend"],
        "execution_supported": payload["execution_supported"],
        "agent_decision": payload["agent_decision"],
        "follow_up_questions": payload["follow_up_questions"],
        "recommended_run_profile": payload["recommended_run_profile"],
        "selected_agent_sequence": payload["selected_agent_sequence"],
        "agent_reply": payload["agent_reply"],
        "analysis_family_route": payload["analysis_family_route"],
        "analysis_family_route_path": payload["analysis_family_route_path"],
        "paper_evidence": payload["paper_evidence"],
        "paper_evidence_path": payload["paper_evidence_path"],
        "paper_spec_surface": payload["paper_spec_surface"],
        "paper_spec_surface_path": payload["paper_spec_surface_path"],
        "analysis_spec_surface": payload["analysis_spec_surface"],
        "analysis_spec_surface_path": payload["analysis_spec_surface_path"],
        "task_build_mode": payload["task_build_mode"],
        "preset": payload["preset"],
        "study_template": payload["study_template"],
        "task_summary": summarize_task_contract(contract),
    }
    if args.run and not payload["missing_high_impact_fields"]:
        runner = AgentRunner(project_root=project_root, config=config)
        execution = runner.run_task(contract, session=session, dry_run=_resolve_dry_run_override(args))
        response["execution"] = execution.as_dict()
        latest_session = LocalRuntime(project_root=project_root).read_session_state(execution.session_id)
        response["status"] = execution.summary.status.value
        response["artifacts"] = [artifact.as_dict() for artifact in latest_session.artifact_records]
        print(json.dumps(response, indent=2, ensure_ascii=False))
        return 0 if execution.summary.status.value == "success" else 2

    print(json.dumps(response, indent=2, ensure_ascii=False))
    return 0


def cmd_run_task(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    config = load_pipeline_config((project_root / args.config).resolve())
    runner = AgentRunner(project_root=project_root, config=config)

    contract: TaskContract
    session: SessionState | None
    if args.session_id:
        session, contract = _load_session_contract(project_root, args.session_id)
    else:
        if not args.paper_path:
            raise SystemExit("--paper-path is required when --session-id is not provided")
        contract, session, payload = _plan_task_flow(
            project_root=project_root,
            config=config,
            paper_path=args.paper_path,
            instructions=_read_instructions(args, project_root),
            session_id=args.session_id,
            use_llm=not args.no_llm,
            interactive=False,
        )
        if payload["missing_high_impact_fields"]:
            print(
                json.dumps(
                    {
                        "session_id": session.session_id,
                        "task_contract_path": session.task_contract_path,
                        "missing_high_impact_fields": payload["missing_high_impact_fields"],
                        "follow_up_questions": payload["follow_up_questions"],
                        "recommended_run_profile": payload["recommended_run_profile"],
                        "agent_reply": payload["agent_reply"],
                        "analysis_family_route": payload["analysis_family_route"],
                        "analysis_family_route_path": payload["analysis_family_route_path"],
                        "paper_evidence": payload["paper_evidence"],
                        "paper_evidence_path": payload["paper_evidence_path"],
                        "paper_spec_surface": payload["paper_spec_surface"],
                        "paper_spec_surface_path": payload["paper_spec_surface_path"],
                        "analysis_spec_surface": payload["analysis_spec_surface"],
                        "analysis_spec_surface_path": payload["analysis_spec_surface_path"],
                        "task_build_mode": payload["task_build_mode"],
                        "task_summary": summarize_task_contract(contract),
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return 2

    execution = runner.run_task(contract, session=session, dry_run=_resolve_dry_run_override(args))
    latest_session = LocalRuntime(project_root=project_root).read_session_state(execution.session_id)
    payload = execution.as_dict()
    payload["status"] = execution.summary.status.value
    payload["artifacts"] = [artifact.as_dict() for artifact in latest_session.artifact_records]
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0 if execution.summary.status.value == "success" else 2


def cmd_export_contract(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)

    if args.session_id:
        _, contract = _load_session_contract(project_root, args.session_id)
    elif args.contract_path:
        contract = TaskContract.from_dict(_read_json_path(project_root, args.contract_path))
    else:
        raise SystemExit("Either --session-id or --contract-path is required")

    payload = contract.as_dict()
    if args.output:
        output_path = (project_root / args.output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def cmd_continue_session(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)
    payload = bridge_continue_session(
        project_root=project_root,
        config_path=(project_root / args.config).resolve(),
        session_id=args.session_id,
        answers=_parse_answer_inputs(args, project_root),
        instructions=_read_optional_instructions(args, project_root),
        run_if_ready=bool(getattr(args, "run", False)),
        dry_run=_resolve_dry_run_override(args),
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if "execution" in payload:
        status = str(payload["execution"]["summary"]["status"])
        return 0 if status == "success" else 2
    return 0


def cmd_lobster_request(args: argparse.Namespace) -> int:
    project_root = _resolve_project_root(getattr(args, "project_root", None))
    _load_project_env(project_root)

    if bool(args.template) and (bool(args.request_file) or bool(args.request_json)):
        raise SystemExit("--template cannot be used together with --request-file or --request-json")

    if args.template:
        payload = bridge_get_lobster_request_template(str(args.template))
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    if bool(args.request_file) and bool(args.request_json):
        raise SystemExit("Use either --request-file or --request-json, not both")
    if not args.request_file and not args.request_json:
        raise SystemExit("Either --template, --request-file, or --request-json is required")

    if args.request_file:
        request_path = Path(args.request_file)
        if not request_path.is_absolute():
            request_path = (project_root / request_path).resolve()
        request_payload = json.loads(request_path.read_text(encoding="utf-8"))
    else:
        request_payload = json.loads(args.request_json)

    if not isinstance(request_payload, dict):
        raise SystemExit("Lobster request payload must be a JSON object")

    payload = bridge_handle_lobster_request(
        project_root=project_root,
        request=request_payload,
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    top_status = str(payload.get("status", "")).strip().lower()
    execution = payload.get("execution")
    if isinstance(execution, dict):
        execution_status = str(execution.get("status", "")).strip().lower()
        if execution_status == "failed":
            return 2
    if top_status == "failed":
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="paper-repro", description="Clinical paper reproduction multi-subagent framework")
    sub = parser.add_subparsers(dest="command", required=True)

    dry = sub.add_parser("dry-run", help="[deprecated] Run the legacy pipeline with synthetic/stub artifacts")
    dry.add_argument("--project-root", type=str, default=".")
    dry.add_argument("--config", type=str, default="configs/pipeline.example.yaml")
    dry.set_defaults(func=cmd_dry_run)

    run = sub.add_parser("run", help="[deprecated] Run the legacy pipeline in production mode")
    run.add_argument("--project-root", type=str, default=".")
    run.add_argument("--config", type=str, default="configs/pipeline.example.yaml")
    run.set_defaults(func=cmd_run)

    run_preset = sub.add_parser(
        "run-preset-pipeline",
        help="[deprecated] Run the legacy deterministic preset pipeline compatibility path",
    )
    run_preset.add_argument("--project-root", type=str, default=".")
    run_preset.add_argument("--config", type=str, default="configs/pipeline.example.yaml")
    run_preset.add_argument("--dry-run", action="store_true")
    run_preset.add_argument("--real-run", action="store_true")
    run_preset.set_defaults(func=cmd_run_preset_pipeline)

    env = sub.add_parser("validate-env", help="Validate required DB environment variables")
    env.add_argument("--project-root", type=str, default=".")
    env.set_defaults(func=cmd_validate_env)

    probe = sub.add_parser("probe-db", help="Probe PostgreSQL connection and visible MIMIC schemas")
    probe.add_argument("--project-root", type=str, default=".")
    probe.set_defaults(func=cmd_probe_db)

    probe_llm = sub.add_parser("probe-llm", help="Probe OpenAI-compatible LLM connectivity and JSON output")
    probe_llm.add_argument("--project-root", type=str, default=".")
    probe_llm.add_argument("--config", type=str, default="configs/agentic.example.yaml")
    probe_llm.add_argument("--model", type=str, default="")
    probe_llm.add_argument("--max-tokens", type=int, default=128)
    probe_llm.add_argument(
        "--prompt",
        type=str,
        default="Return JSON confirming that the LLM connection for clinical paper planning is working.",
    )
    probe_llm.set_defaults(func=cmd_probe_llm)

    extract = sub.add_parser(
        "extract-analysis-dataset",
        help="Build the paper-aligned analysis dataset and missingness report",
    )
    extract.add_argument("--project-root", type=str, default=".")
    extract.add_argument("--profile", type=str, default="mimic_tyg_sepsis")
    extract.add_argument("--admit-year-start", type=int, default=None)
    extract.add_argument("--admit-year-end", type=int, default=None)
    extract.add_argument("--output", type=str, default="shared/analysis_dataset.csv")
    extract.add_argument("--missingness-output", type=str, default="shared/analysis_missingness.json")
    extract.add_argument("--sepsis-source", choices=["auto", "derived", "icd"], default="auto")
    extract.set_defaults(func=cmd_extract_analysis_dataset)

    describe = sub.add_parser("describe-openclaw", help="Describe the OpenClaw integration contract and recommended assets")
    describe.add_argument("--project-root", type=str, default=".")
    describe.set_defaults(func=cmd_describe_openclaw)

    describe_skills = sub.add_parser("describe-skills", help="Describe machine-readable OpenClaw skill contracts")
    describe_skills.add_argument("--project-root", type=str, default=".")
    describe_skills.set_defaults(func=cmd_describe_skills)

    chat = sub.add_parser("chat", help="Build a structured task contract from a paper plus free-form instructions")
    chat.add_argument("--project-root", type=str, default=".")
    chat.add_argument("--config", type=str, default="configs/agentic.example.yaml")
    chat.add_argument("--paper-path", type=str, required=True)
    chat.add_argument("--instructions", type=str, default="")
    chat.add_argument("--instructions-file", type=str, default="")
    chat.add_argument("--session-id", type=str, default="")
    chat.add_argument("--no-llm", action="store_true")
    chat.add_argument("--no-prompt", action="store_true")
    chat.add_argument("--run", action="store_true")
    chat.add_argument("--dry-run", action="store_true")
    chat.add_argument("--real-run", action="store_true")
    chat.set_defaults(func=cmd_chat)

    plan = sub.add_parser("plan-task", help="Create and persist a task contract without executing it")
    plan.add_argument("--project-root", type=str, default=".")
    plan.add_argument("--config", type=str, default="configs/agentic.example.yaml")
    plan.add_argument("--paper-path", type=str, required=True)
    plan.add_argument("--instructions", type=str, default="")
    plan.add_argument("--instructions-file", type=str, default="")
    plan.add_argument("--session-id", type=str, default="")
    plan.add_argument("--no-llm", action="store_true")
    plan.set_defaults(func=cmd_plan_task)

    continue_session = sub.add_parser(
        "continue-session",
        help="Apply follow-up answers to an existing session, refresh the contract, and optionally run if ready",
    )
    continue_session.add_argument("--project-root", type=str, default=".")
    continue_session.add_argument("--config", type=str, default="configs/agentic.example.yaml")
    continue_session.add_argument("--session-id", type=str, required=True)
    continue_session.add_argument(
        "--answer",
        action="append",
        default=[],
        help="Follow-up answer in the form field=value. Can be passed multiple times.",
    )
    continue_session.add_argument("--answers-file", type=str, default="")
    continue_session.add_argument("--instructions", type=str, default="")
    continue_session.add_argument("--instructions-file", type=str, default="")
    continue_session.add_argument("--run", action="store_true")
    continue_session.add_argument("--dry-run", action="store_true")
    continue_session.add_argument("--real-run", action="store_true")
    continue_session.set_defaults(func=cmd_continue_session)

    lobster = sub.add_parser(
        "lobster-request",
        help="Handle a single Lobster/OpenClaw request object and auto-route plan/continue/run",
    )
    lobster.add_argument("--project-root", type=str, default=".")
    lobster.add_argument(
        "--template",
        choices=["plan_only", "agentic_repro", "follow_up"],
        default="",
        help="Print a request JSON template and exit",
    )
    lobster.add_argument("--request-file", type=str, default="")
    lobster.add_argument("--request-json", type=str, default="")
    lobster.set_defaults(func=cmd_lobster_request)

    run_task = sub.add_parser("run-task", help="Execute a planned task contract through the multi-subagent runner")
    run_task.add_argument("--project-root", type=str, default=".")
    run_task.add_argument("--config", type=str, default="configs/agentic.example.yaml")
    run_task.add_argument("--session-id", type=str, default="")
    run_task.add_argument("--paper-path", type=str, default="")
    run_task.add_argument("--instructions", type=str, default="")
    run_task.add_argument("--instructions-file", type=str, default="")
    run_task.add_argument("--no-llm", action="store_true")
    run_task.add_argument("--dry-run", action="store_true")
    run_task.add_argument("--real-run", action="store_true")
    run_task.set_defaults(func=cmd_run_task)

    export = sub.add_parser("export-contract", help="Print or write a persisted task contract")
    export.add_argument("--project-root", type=str, default=".")
    export.add_argument("--session-id", type=str, default="")
    export.add_argument("--contract-path", type=str, default="")
    export.add_argument("--output", type=str, default="")
    export.set_defaults(func=cmd_export_contract)
    return parser


def _read_instructions(args: argparse.Namespace, project_root: Path) -> str:
    if getattr(args, "instructions", ""):
        return str(args.instructions).strip()
    instructions_file = getattr(args, "instructions_file", "")
    if instructions_file:
        path = Path(instructions_file)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        return path.read_text(encoding="utf-8")
    raise SystemExit("Instructions are required via --instructions or --instructions-file")


def _resolve_dry_run_override(args: argparse.Namespace) -> bool | None:
    dry_run = bool(getattr(args, "dry_run", False))
    real_run = bool(getattr(args, "real_run", False))
    if dry_run and real_run:
        raise SystemExit("--dry-run and --real-run cannot be used together")
    if dry_run:
        return True
    if real_run:
        return False
    return None


def _read_optional_instructions(args: argparse.Namespace, project_root: Path) -> str:
    if getattr(args, "instructions", ""):
        return str(args.instructions).strip()
    instructions_file = getattr(args, "instructions_file", "")
    if not instructions_file:
        return ""
    path = Path(instructions_file)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path.read_text(encoding="utf-8").strip()


def _read_json_path(project_root: Path, path_str: str) -> dict:
    path = Path(path_str)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_json_path(project_root: Path, path_str: str) -> dict:
    if not str(path_str).strip():
        return {}
    path = Path(path_str)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_session_contract(project_root: Path, session_id: str) -> tuple[SessionState, TaskContract]:
    runtime = LocalRuntime(project_root=project_root)
    session = runtime.read_session_state(session_id)
    contract_payload = _read_json_path(project_root, session.task_contract_path)
    return session, TaskContract.from_dict(contract_payload)


def _plan_task_flow(
    *,
    project_root: Path,
    config: PipelineConfig,
    paper_path: str,
    instructions: str,
    session_id: str,
    use_llm: bool,
    interactive: bool,
) -> tuple[TaskContract, SessionState, dict[str, object]]:
    task_result = build_task_contract(
        project_root=project_root,
        config=config,
        paper_path=paper_path,
        instructions=instructions,
        session_id=session_id,
        use_llm=use_llm,
    )
    contract = task_result.contract
    missing_high_impact_fields = find_missing_high_impact_fields(contract)

    if interactive and missing_high_impact_fields and sys.stdin.isatty():
        answers = _prompt_for_missing_fields(missing_high_impact_fields)
        if answers:
            contract = normalize_task_contract(
                apply_follow_up_answers(contract, answers),
                config=config,
                project_root=project_root,
            )
            missing_high_impact_fields = find_missing_high_impact_fields(contract)

    runner = AgentRunner(project_root=project_root, config=config)
    session = runner.create_session(
        contract,
        paper_path=paper_path,
        instructions=instructions,
        session_id=session_id,
    )
    _persist_cli_task_build_artifacts(runtime=runner.runtime, session=session, task_result=task_result)
    decision = runner.prepare_agent_decision(contract, session)
    support = get_dataset_adapter(contract.dataset.adapter).describe_contract(contract)
    preset = get_paper_preset(contract.meta.get("preset"))
    template = infer_study_template(contract)
    return contract, session, {
        "missing_high_impact_fields": list(decision.missing_high_impact_fields),
        "used_llm": task_result.used_llm,
        "llm_error": task_result.llm_error,
        "execution_backend": support.execution_backend,
        "execution_supported": support.execution_supported,
        "agent_decision": decision.as_dict(),
        "follow_up_questions": [item.as_dict() for item in decision.follow_up_questions],
        "recommended_run_profile": decision.recommended_run_profile,
        "selected_agent_sequence": list(decision.selected_agent_sequence),
        "agent_reply": _read_agent_reply(project_root, session),
        "analysis_family_route": dict(decision.analysis_family_route),
        "analysis_family_route_path": str(session.meta.get("analysis_family_route_path", "")),
        "preset": preset.as_dict() if preset is not None else None,
        "study_template": template.as_dict() if template is not None else None,
        "paper_evidence": task_result.paper_evidence,
        "paper_evidence_path": str(session.meta.get("paper_evidence_path", "")),
        "paper_spec_surface": _read_optional_json_path(project_root, str(session.meta.get("paper_spec_surface_path", ""))),
        "paper_spec_surface_path": str(session.meta.get("paper_spec_surface_path", "")),
        "analysis_spec_surface": _read_optional_json_path(
            project_root,
            str(session.meta.get("analysis_spec_surface_path", "")),
        ),
        "analysis_spec_surface_path": str(session.meta.get("analysis_spec_surface_path", "")),
        "task_build_mode": str(session.meta.get("task_build_mode", "")),
    }


def _persist_cli_task_build_artifacts(
    *,
    runtime: LocalRuntime,
    session: SessionState,
    task_result: object,
) -> None:
    build_mode = "deterministic_only"
    if bool(getattr(task_result, "used_llm", False)):
        build_mode = "hybrid_llm_assisted"
    elif str(getattr(task_result, "llm_error", "")).strip():
        build_mode = "deterministic_fallback_after_llm_error"
    session.meta["task_build_mode"] = build_mode

    paper_evidence = getattr(task_result, "paper_evidence", None)
    if isinstance(paper_evidence, dict) and paper_evidence:
        rel_path = f"shared/sessions/{session.session_id}/paper_evidence.json"
        runtime.write_json(rel_path, paper_evidence)
        session.meta["paper_evidence_path"] = rel_path
    runtime.write_session_state(session)


def _prompt_for_missing_fields(missing_fields: list[str]) -> dict[str, str]:
    prompts = {
        "exposure_variables": "Exposure variables",
        "outcome_variables": "Outcome variables",
        "control_variables": "Control variables",
        "models": "Models to run",
        "outputs": "Requested outputs",
        "cohort_logic": "Cohort logic",
    }
    answers: dict[str, str] = {}
    for field in missing_fields:
        prompt = prompts.get(field, field)
        value = input(f"{prompt}: ").strip()
        if value:
            answers[field] = value
    return answers


def _parse_answer_inputs(args: argparse.Namespace, project_root: Path) -> dict[str, str]:
    answers: dict[str, str] = {}
    answers_file = getattr(args, "answers_file", "")
    if answers_file:
        path = Path(answers_file)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise SystemExit("--answers-file must contain a JSON object")
        for key, value in payload.items():
            key_text = str(key).strip()
            value_text = str(value).strip()
            if key_text and value_text:
                answers[key_text] = value_text
    for raw in getattr(args, "answer", []) or []:
        key, sep, value = str(raw).partition("=")
        if not sep:
            raise SystemExit(f"Invalid --answer value '{raw}'. Use field=value.")
        key_text = key.strip()
        value_text = value.strip()
        if not key_text or not value_text:
            raise SystemExit(f"Invalid --answer value '{raw}'. Use field=value.")
        answers[key_text] = value_text
    return answers


def _read_agent_reply(project_root: Path, session: SessionState) -> str:
    rel_path = str(session.meta.get("agent_reply_path", "")).strip()
    if not rel_path:
        return ""
    path = (project_root / rel_path).resolve()
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
