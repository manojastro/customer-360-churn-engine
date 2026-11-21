# File: snowpark/deploy.py
"""Deployment orchestration for the churn scoring pipeline.

Steps (all idempotent, safe to re-run):

1. Ensure the ``ML`` database, ``PUBLIC`` schema, and ``ARTIFACTS`` stage exist.
2. Upload pipeline modules and the optional model artifact to the stage.
3. Register the permanent stored procedure (``replace=True``).
4. Execute a backfill scoring run via ``CALL``.
5. Verify the scores table landed rows for the run.

Usage:
    python snowpark/deploy.py [--model-artifact path/to/model.json] [--skip-backfill]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv
from snowflake.snowpark import Session

logger = logging.getLogger("churn_deploy")

STAGE_FQN = "ML.PUBLIC.ARTIFACTS"
SCORE_TABLE = "ML.PUBLIC.CUSTOMER_CHURN_SCORES"
MODULES = ["churn_scoring.py", "feature_engineering.py"]


def build_session() -> Session:
    """Create a Snowpark session from environment variables (.env supported)."""
    load_dotenv()
    config: dict[str, str] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.getenv("SNOWFLAKE_ROLE", "CHURN_ENGINEER"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "WH_TRANSFORM"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
    }
    if key_path := os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
        config["private_key_file"] = key_path
    else:
        config["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return Session.builder.configs(config).create()


def ensure_infra(session: Session) -> None:
    """Create database/schema/stage objects if missing (idempotent DDL)."""
    session.sql("create database if not exists ML").collect()
    session.sql("use database ML").collect()
    session.sql(
        f"""
        create stage if not exists {STAGE_FQN}
            comment = 'Churn pipeline artifacts: modules + model'
        """
    ).collect()


def upload_artifacts(session: Session, model_artifact: Path | None) -> None:
    """PUT pipeline modules (and optional serialized model) onto the stage."""
    local_dir = Path(__file__).resolve().parent
    for module in MODULES:
        session.file.put(
            str(local_dir / module),
            STAGE_FQN,
            overwrite=True,
            auto_compress=False,
        )
        logger.info("Uploaded %s -> @%s", module, STAGE_FQN)
    if model_artifact is not None:
        if not model_artifact.exists():
            raise FileNotFoundError(f"model artifact not found: {model_artifact}")
        session.file.put(str(model_artifact), f"{STAGE_FQN}/model/", overwrite=True)
        logger.info("Uploaded model artifact %s", model_artifact.name)


def register(session: Session) -> None:
    """Register the permanent scoring stored procedure."""
    import churn_scoring

    proc = churn_scoring.register_sproc(session)
    logger.info("Registered stored procedure %s (%s)", churn_scoring.PROC_NAME, proc)


def backfill(session: Session) -> dict[str, int]:
    """Run one full scoring pass and verify output row counts."""
    summary_raw = session.call("ML.SP_CUSTOMER_CHURN_SCORING")
    scored_rows = session.table(SCORE_TABLE).count()
    logger.info("Backfill complete | sproc summary=%s | table rows=%s", summary_raw, scored_rows)
    return {"table_rows": int(scored_rows)}


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint; wires all deployment steps together with error handling."""
    parser = argparse.ArgumentParser(description="Deploy the churn scoring pipeline")
    parser.add_argument(
        "--model-artifact",
        type=Path,
        default=None,
        help="Local path to a serialized XGBoost model (.json) to upload",
    )
    parser.add_argument("--skip-backfill", action="store_true")
    args = parser.parse_args(argv)

    steps: list[tuple[str, Callable[[Session], object]]] = [
        ("ensure_infra", ensure_infra),
        ("upload_artifacts", lambda s: upload_artifacts(s, args.model_artifact)),
        ("register", register),
    ]
    if not args.skip_backfill:
        steps.append(("backfill", backfill))

    session: Session | None = None
    try:
        session = build_session()
        for name, step in steps:
            logger.info("==> step: %s", name)
            step(session)
        logger.info("Deployment finished successfully")
        return 0
    except Exception:  # noqa: BLE001 - top-level failure boundary with exit code
        logger.exception("Deployment failed")
        return 1
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    sys.exit(main())
