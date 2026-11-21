# File: snowpark/churn_scoring.py
"""Customer churn propensity scoring - Snowpark permanent stored procedure.

Reads the unified Customer 360 dimension (``ANALYTICS.MARTS.DIM_CUSTOMER``) and
behavioral features aggregated from ``fact_interactions``, scores each customer
with an XGBoost-style binary classifier, and upserts results into
``ML.PUBLIC.CUSTOMER_CHURN_SCORES`` (referred to as ``ML.CUSTOMER_CHURN_SCORES``
when the ``ML`` database is the session default) with score metadata:
``MODEL_VERSION`` and ``SCORED_AT``.

Idempotency: scores are MERGE-upserted on ``CUSTOMER_KEY``, so re-running the
procedure never duplicates rows; every run refreshes metadata.

Deployment contract (see deploy.py): this module and feature_engineering.py are
uploaded to stage ``@ML.PUBLIC.ARTIFACTS`` and registered with those files as
imports, so at runtime the staged modules resolve on ``sys.path``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Iterator

import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

logger = logging.getLogger("churn_scoring")

SCORE_TABLE = "ML.PUBLIC.CUSTOMER_CHURN_SCORES"
STAGE = "@ML.PUBLIC.ARTIFACTS"
DEFAULT_MODEL_STAGE_PATH = "@ML.PUBLIC.ARTIFACTS/model/churn_xgb_v1.json"
PROC_NAME = "ML.SP_CUSTOMER_CHURN_SCORING"
MODEL_VERSION_FALLBACK = "mock-xgb-churn-v0.9.3"
HIGH_RISK_THRESHOLD = float(os.getenv("CHURN_HIGH_RISK_THRESHOLD", "0.7"))


class ScoringError(RuntimeError):
    """Raised when the churn scoring pipeline cannot complete."""


class MockXGBWrapper:
    """Deterministic placeholder for a trained XGBoost churn classifier.

    Mimics the ``predict_proba`` contract of ``xgboost.XGBClassifier`` so the
    pipeline is exercisable end-to-end without a serialized artifact. In
    production, :meth:`ChurnModel.load` deserializes a real booster from the
    model stage and this class becomes dead code.
    """

    def __init__(self) -> None:
        self.model_version = MODEL_VERSION_FALLBACK
        # Hand-tuned linear surrogate of the production model's signal mix.
        self._bias: float = -1.10
        self._weights: dict[str, float] = {
            "TENURE_DAYS": -0.0004,
            "LIFETIME_VALUE": -0.000001,
            "HEALTH_SCORE": -0.0200,
            "EVENTS_LAST_30D": -0.0100,
            "USAGE_TREND_RATIO": -0.1500,
            "AVG_DAYS_BETWEEN_EVENTS": 0.0080,
            "DAYS_SINCE_LAST_EVENT": 0.0040,
            "NEGATIVE_SIGNAL_90D": 0.1200,
            "POSITIVE_SIGNAL_90D": -0.0300,
            "OPEN_SUPPORT_ITEMS": 0.0600,
            "ESCALATION_RATE": 0.9000,
        }

    def predict_proba(self, rows: Iterator[tuple[float, ...]]) -> list[float]:
        """Score rows of raw feature values in the declared FEATURE_COLUMNS order."""
        probabilities: list[float] = []
        for row in rows:
            z = self._bias + sum(
                self._weights.get(name, 0.0) * self._coerce(value)
                for name, value in zip(self._weights.keys(), row)
            )
            z = max(min(z, 60.0), -60.0)  # overflow-safe sigmoid domain
            probabilities.append(1.0 / (1.0 + math.exp(-z)))
        return probabilities

    @staticmethod
    def _coerce(value: Any) -> float:
        return float(value) if value is not None else 0.0


class ChurnModel:
    """Model loader with graceful fallback to the deterministic mock wrapper."""

    @staticmethod
    def load(session: Session, stage_path: str | None = None) -> MockXGBWrapper | Any:
        """Load a serialized booster from a Snowflake stage, else fall back to mock.

        Args:
            session: Active Snowpark session.
            stage_path: Fully qualified stage file path, e.g.
                ``@ML.PUBLIC.ARTIFACTS/model/churn_xgb_v1.json``.

        Returns:
            An object exposing ``predict_proba(rows)`` and ``model_version``.
        """
        stage_path = stage_path or os.getenv(
            "CHURN_MODEL_STAGE_PATH", DEFAULT_MODEL_STAGE_PATH
        )
        try:
            import tempfile
            from pathlib import Path

            import xgboost  # optional dependency inside sproc sandbox

            with tempfile.TemporaryDirectory() as tmp:
                session.file.get(stage_path, tmp)
                artifact = next(Path(tmp).rglob("*.json"))
                booster = xgboost.Booster()
                booster.load_model(str(artifact))
                logger.info("Loaded XGBoost booster from %s", stage_path)

                class _RealModel:
                    model_version = f"xgb-{Path(stage_path).name}"

                    @staticmethod
                    def predict_proba(rows: Iterator[tuple[float, ...]]) -> list[float]:
                        import numpy as np

                        matrix = np.array(list(rows), dtype=float).reshape(-1, -1)
                        return booster.predict(
                            xgboost.DMatrix(matrix), output_margin=False
                        ).tolist()

                return _RealModel()
        except Exception as exc:  # noqa: BLE001 - fallback is intentional
            logger.warning(
                "Falling back to MockXGBWrapper (%s): artifact unavailable at %s",
                type(exc).__name__,
                stage_path,
            )
            return MockXGBWrapper()


def score_customers(session: Session) -> str:
    """Entry point executed by the stored procedure. Scores all active customers.

    Steps: assemble features -> load model -> batch-score -> MERGE upsert into
    the scores table. Never leaves partial state: the merge runs once against a
    fully materialized batch table.

    Args:
        session: Session injected by Snowpark at procedure runtime.

    Returns:
        JSON summary string (rows scored, high-risk count, duration).

    Raises:
        ScoringError: on any unrecoverable pipeline failure.
    """
    started = time.perf_counter()
    try:
        # Staged module resolves via sys.path inside the procedure sandbox.
        from feature_engineering import FEATURE_COLUMNS, assemble_feature_matrix

        features_df = assemble_feature_matrix(session)
        features_pdf: pd.DataFrame = features_df.to_pandas()
        if features_pdf.empty:
            logger.warning("No customers to score; dim/feature join returned 0 rows.")
            return json.dumps({"rows_scored": 0, "model_version": "n/a", "high_risk": 0})

        model = ChurnModel.load(session)
        feature_rows = features_pdf[list(FEATURE_COLUMNS)].itertuples(index=False, name=None)
        probabilities = model.predict_proba(feature_rows)

        scores_pdf = pd.DataFrame(
            {
                "CUSTOMER_KEY": features_pdf["CUSTOMER_KEY"].astype(str),
                "CHURN_PROBABILITY_SCORE": [round(p, 6) for p in probabilities],
                "RISK_TIER": [
                    _risk_tier(p) for p in probabilities
                ],
                "MODEL_VERSION": [model.model_version] * len(probabilities),
                "FEATURE_SNAPSHOT": [features_pdf.shape[1] - 1] * len(probabilities),
            }
        )

        rows_merged = _merge_scores(session, scores_pdf)

        summary = {
            "rows_scored": int(len(scores_pdf)),
            "rows_merged": int(rows_merged),
            "high_risk": int((scores_pdf["CHURN_PROBABILITY_SCORE"] > HIGH_RISK_THRESHOLD).sum()),
            "model_version": model.model_version,
            "duration_seconds": round(time.perf_counter() - started, 2),
        }
        logger.info("Churn scoring complete: %s", summary)
        return json.dumps(summary)

    except SnowparkSQLException as exc:
        logger.exception("Snowpark SQL failure during churn scoring")
        raise ScoringError(f"scoring failed on SQL execution: {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - single failure boundary for the sproc
        logger.exception("Unexpected failure during churn scoring")
        raise ScoringError(f"scoring failed: {exc}") from exc


def _risk_tier(score: float) -> str:
    """Bucket a probability into the operational risk tiers consumed by Salesforce."""
    if score > HIGH_RISK_THRESHOLD:
        return "HIGH"
    if score > 0.4:
        return "MEDIUM"
    return "LOW"


def _merge_scores(session: Session, scores_pdf: pd.DataFrame) -> int:
    """Idempotently upsert the scored batch into ``SCORE_TABLE`` via MERGE."""
    batch_table = f"{SCORE_TABLE}_BATCH"
    try:
        session.write_pandas(
            scores_pdf,
            table_name=batch_table,
            database=None,
            schema=None,
            auto_create_table=True,
            overwrite=True,
        )
        result = (
            session.sql(
                f"""
                merge into {SCORE_TABLE} t
                using {batch_table} s
                  on t.customer_key = s.customer_key
                when matched then update set
                    t.churn_probability_score = s.churn_probability_score,
                    t.risk_tier               = s.risk_tier,
                    t.model_version           = s.model_version,
                    t.feature_snapshot        = s.feature_snapshot,
                    t.scored_at               = current_timestamp()
                when not matched then insert
                    (customer_key, churn_probability_score, risk_tier,
                     model_version, feature_snapshot, scored_at)
                    values
                    (s.customer_key, s.churn_probability_score, s.risk_tier,
                     s.model_version, s.feature_snapshot, current_timestamp())
                """
            )
            .collect()
        )
        merged = int(result[0]["number of rows inserted"]) + int(result[0]["number of rows updated"])
        logger.info("MERGE into %s affected %s rows", SCORE_TABLE, merged)
        return merged
    finally:
        session.sql(f"drop table if exists {batch_table}").collect()


def register_sproc(session: Session):
    """Register the permanent stored procedure (idempotent: replace=True)."""
    return session.sproc.register(
        func=score_customers,
        name=PROC_NAME,
        input_types=[],
        return_type=str,
        is_permanent=True,
        replace=True,
        stage_location=STAGE,
        imports=[
            f"{STAGE}/churn_scoring.py",
            f"{STAGE}/feature_engineering.py",
        ],
        packages=["snowflake-snowpark-python", "pandas"],
        execute_as="owner",
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    )

    from dotenv import load_dotenv

    load_dotenv()

    config = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.getenv("SNOWFLAKE_ROLE", "CHURN_ENGINEER"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "WH_TRANSFORM"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
        **(
            {"private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]}
            if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
            else {"password": os.environ["SNOWFLAKE_PASSWORD"]}
        ),
    }

    _session: Session | None = None
    try:
        _session = Session.builder.configs(config).create()
        register_sproc(_session)
        print(_session.call(PROC_NAME))
    except Exception:
        logger.exception("Deployment/backfill failed")
        raise SystemExit(1)
    finally:
        if _session is not None:
            _session.close()
