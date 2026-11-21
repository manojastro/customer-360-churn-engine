# File: snowpark/feature_engineering.py
"""Snowpark feature engineering for the churn scoring pipeline.

Builds the model-ready feature matrix entirely in-warehouse (no data egress):

* ``build_behavioral_features`` - conditional aggregations over
  ``fact_interactions`` (trailing-window event counts, negative-signal counts)
  plus window-function gap statistics (average inter-event interval).
* ``assemble_feature_matrix`` - joins static profile features from
  ``dim_customer`` with behavioral aggregates, imputes nulls, and performs
  VectorAssembler-style manual assembly into an ordered numeric vector.

All public functions are pure (no side effects) and accept an injected
:class:`~snowflake.snowpark.Session`, which keeps them unit-testable against
local sessions.
"""

from __future__ import annotations

import logging

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window

logger = logging.getLogger(__name__)

DIM_CUSTOMER = "ANALYTICS.MARTS.DIM_CUSTOMER"
FACT_INTERACTIONS = "ANALYTICS.MARTS.FACT_INTERACTIONS"

#: Ordered feature contract shared by training and inference. The order MUST
#: match the serialized model artifact; changing it requires bumping
#: ``MODEL_VERSION`` in churn_scoring.py.
FEATURE_COLUMNS: tuple[str, ...] = (
    "TENURE_DAYS",
    "LIFETIME_VALUE",
    "HEALTH_SCORE",
    "EVENTS_LAST_30D",
    "EVENTS_PRIOR_30D",
    "USAGE_TREND_RATIO",
    "AVG_DAYS_BETWEEN_EVENTS",
    "DAYS_SINCE_LAST_EVENT",
    "NEGATIVE_SIGNAL_90D",
    "POSITIVE_SIGNAL_90D",
    "OPEN_SUPPORT_ITEMS",
    "ESCALATION_RATE",
)


def build_behavioral_features(session: Session) -> DataFrame:
    """Aggregate customer behavior from ``fact_interactions``.

    Uses conditional aggregation (``SUM(IFF(<predicate>, 1, 0))``) for
    trailing-window buckets and a ``LAG`` window function to compute the mean
    inter-event gap per customer.

    Args:
        session: Active Snowpark session.

    Returns:
        One row per known ``CUSTOMER_KEY`` with the behavioral feature columns.
    """
    interactions = (
        session.table(FACT_INTERACTIONS)
        .filter(F.col("CUSTOMER_KEY").is_not_null())
        .select(
            F.col("CUSTOMER_KEY"),
            F.col("INTERACTION_TS"),
            F.col("SENTIMENT_CLASS"),
            F.col("EVENT_TYPE"),
        )
    )

    # Window function: days elapsed between consecutive interactions.
    by_customer_time = Window.partition_by("CUSTOMER_KEY").order_by(
        F.col("INTERACTION_TS")
    )
    with_gaps = (
        interactions.with_column(
            "PREV_EVENT_TS",
            F.lag(F.col("INTERACTION_TS")).over(by_customer_time),
        ).with_column(
            "GAP_DAYS",
            F.datediff("day", F.col("PREV_EVENT_TS"), F.col("INTERACTION_TS")),
        )
    )

    now = F.current_timestamp()

    def days_ago(n: int):
        """Column expression: whole days between the event timestamp and now."""
        return F.datediff("day", F.col("INTERACTION_TS"), now)

    behavioral = with_gaps.group_by("CUSTOMER_KEY").agg(
        F.sum(F.iff(days_ago(30) <= 30, 1, 0)).alias("EVENTS_LAST_30D"),
        F.sum(
            F.iff((days_ago(60) <= 60) & (days_ago(60) > 30), 1, 0)
        ).alias("EVENTS_PRIOR_30D"),
        F.sum(F.iff(days_ago(90) <= 90, 1, 0)).alias("EVENTS_90D"),
        F.sum(
            F.iff((days_ago(90) <= 90) & (F.col("SENTIMENT_CLASS") == "negative"), 1, 0)
        ).alias("NEGATIVE_SIGNAL_90D"),
        F.sum(
            F.iff((days_ago(90) <= 90) & (F.col("SENTIMENT_CLASS") == "positive"), 1, 0)
        ).alias("POSITIVE_SIGNAL_90D"),
        F.avg(F.col("GAP_DAYS")).alias("AVG_DAYS_BETWEEN_EVENTS"),
        F.max(days_ago(3650)).alias("DAYS_SINCE_LAST_EVENT"),
        F.count(F.lit(1)).alias("LIFETIME_EVENTS"),
    )

    # Guarded trend ratio: recent pace vs. prior pace, floored to avoid /0.
    behavioral = behavioral.with_column(
        "USAGE_TREND_RATIO",
        F.col("EVENTS_LAST_30D")
        / F.greatest(F.col("EVENTS_PRIOR_30D"), F.lit(1.0)),
    )

    logger.debug("behavioral aggregates built for %s customers", behavioral.count())  # pragma: no cover - debug aid
    return behavioral


def assemble_feature_matrix(session: Session) -> DataFrame:
    """Join profile features with behavioral aggregates into a scored-ready matrix.

    Null-safe: every feature column is coalesced to a neutral default before
    assembly so the downstream scorer never sees NULLs. A VectorAssembler-style
    ``FEATURE_VECTOR`` array column is emitted alongside the named columns.

    Args:
        session: Active Snowpark session.

    Returns:
        DataFrame keyed by ``CUSTOMER_KEY`` with all :data:`FEATURE_COLUMNS`
        (as doubles) plus ``FEATURE_VECTOR``.
    """
    profiles = session.table(DIM_CUSTOMER).select(
        F.col("CUSTOMER_KEY"),
        F.coalesce(F.col("TENURE_DAYS"), F.lit(0)).cast("double").alias("TENURE_DAYS"),
        F.coalesce(F.col("LIFETIME_VALUE"), F.lit(0)).cast("double").alias("LIFETIME_VALUE"),
        F.coalesce(F.col("HEALTH_SCORE"), F.lit(50)).cast("double").alias("HEALTH_SCORE"),
        F.coalesce(F.col("OPEN_CASES"), F.lit(0)).alias("_OPEN_CASES"),
        F.coalesce(F.col("OPEN_TICKETS"), F.lit(0)).alias("_OPEN_TICKETS"),
        F.coalesce(F.col("TOTAL_CASES"), F.lit(0)).alias("_TOTAL_CASES"),
        F.coalesce(F.col("OPEN_ESCALATED_CASES"), F.lit(0)).alias("_OPEN_ESC_CASES"),
    )

    behavioral = build_behavioral_features(session)

    joined = profiles.join(behavioral, on="CUSTOMER_KEY", how="inner")

    matrix = joined.select(
        F.col("CUSTOMER_KEY"),
        *[F.col(name) for name in FEATURE_COLUMNS[:3]],
        F.coalesce(F.col("EVENTS_LAST_30D"), F.lit(0)).cast("double").alias("EVENTS_LAST_30D"),
        F.coalesce(F.col("EVENTS_PRIOR_30D"), F.lit(0)).cast("double").alias("EVENTS_PRIOR_30D"),
        F.least(F.coalesce(F.col("USAGE_TREND_RATIO"), F.lit(1.0)), F.lit(5.0))
            .cast("double").alias("USAGE_TREND_RATIO"),
        F.coalesce(F.col("AVG_DAYS_BETWEEN_EVENTS"), F.lit(30)).cast("double").alias("AVG_DAYS_BETWEEN_EVENTS"),
        F.coalesce(F.col("DAYS_SINCE_LAST_EVENT"), F.lit(999)).cast("double").alias("DAYS_SINCE_LAST_EVENT"),
        F.coalesce(F.col("NEGATIVE_SIGNAL_90D"), F.lit(0)).cast("double").alias("NEGATIVE_SIGNAL_90D"),
        F.coalesce(F.col("POSITIVE_SIGNAL_90D"), F.lit(0)).cast("double").alias("POSITIVE_SIGNAL_90D"),
        (F.col("_OPEN_CASES") + F.col("_OPEN_TICKETS")).cast("double").alias("OPEN_SUPPORT_ITEMS"),
        F.when(F.col("_TOTAL_CASES") > 0, F.col("_OPEN_ESC_CASES") / F.col("_TOTAL_CASES"))
            .otherwise(F.lit(0.0))
            .cast("double")
            .alias("ESCALATION_RATE"),
    )

    # VectorAssembler-style manual assembly into a fixed-order numeric vector.
    return matrix.with_column(
        "FEATURE_VECTOR",
        F.array_construct(*[F.col(name) for name in FEATURE_COLUMNS]),
    )
