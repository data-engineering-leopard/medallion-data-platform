import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, TimestampType

logger = logging.getLogger(__name__)

MAX_DATE = "9999-12-31 00:00:00"


def apply_scd2(
    spark: SparkSession,
    incoming_df: DataFrame,
    existing_df: DataFrame,
    scd2_key: str,
    track_columns: list
) -> DataFrame:
    """
    Applies SCD2 logic to merge incoming data with existing data.

    For each incoming record:
    - If it is brand new → insert with is_current=True
    - If it exists and tracked columns have changed → close old version,
      insert new version
    - If it exists and nothing has changed → keep as is, no duplicate

    Returns the full updated DataFrame including all historical versions.
    """

    # ===========================
    # STEP 1: Add SCD2 columns to incoming records
    # These are brand new versions so they are all current
    # ===========================
    incoming_with_meta = (
        incoming_df
        .withColumn("effective_from", F.current_timestamp())
        .withColumn(
            "effective_to",
            F.to_timestamp(F.lit(MAX_DATE))
        )
        .withColumn("is_current", F.lit(True).cast(BooleanType()))
    )

    # ===========================
    # STEP 2: Handle empty existing table
    # If there is no existing data, all incoming records are new
    # ===========================
    if existing_df.count() == 0:
        logger.info(
            f"No existing data found — inserting "
            f"{incoming_with_meta.count()} new records"
        )
        return incoming_with_meta

    # ===========================
    # STEP 3: Identify changed records
    # Join incoming against current existing records and compare
    # tracked columns to find what has actually changed
    # ===========================
    current_existing = existing_df.filter(F.col("is_current") == True)

    # Build a condition that detects any change in any tracked column
    change_condition = None
    for col in track_columns:
        condition = (
            F.col(f"incoming.{col}") != F.col(f"existing.{col}")
        ) | (
            F.col(f"incoming.{col}").isNull() !=
            F.col(f"existing.{col}").isNull()
        )
        change_condition = (
            condition if change_condition is None
            else change_condition | condition
        )

    incoming_aliased = incoming_with_meta.alias("incoming")
    existing_aliased = current_existing.alias("existing")

    joined = incoming_aliased.join(
        existing_aliased,
        on=F.col(f"incoming.{scd2_key}") == F.col(f"existing.{scd2_key}"),
        how="left"
    )

    # Records where something tracked has changed
    changed = joined.filter(change_condition).select("incoming.*")

    # Records that are completely new (no match in existing)
    new_records = joined.filter(
        F.col(f"existing.{scd2_key}").isNull()
    ).select("incoming.*")

    # Records where nothing has changed — we keep existing, discard incoming
    unchanged_keys = joined.filter(
        ~change_condition
    ).select(F.col(f"incoming.{scd2_key}").alias(scd2_key))

    # ===========================
    # STEP 4: Close old versions of changed records
    # Set effective_to to now and is_current to False
    # ===========================
    changed_keys = changed.select(scd2_key)

    closed_records = (
        current_existing
        .join(changed_keys, on=scd2_key, how="inner")
        .withColumn("effective_to", F.current_timestamp())
        .withColumn("is_current", F.lit(False).cast(BooleanType()))
    )

    # ===========================
    # STEP 5: Assemble the final DataFrame
    # - Historical records (already closed, is_current=False)
    # - Newly closed records (old versions of changed records)
    # - New versions of changed records
    # - Brand new records
    # - Unchanged records (kept as is)
    # ===========================
    historical = existing_df.filter(F.col("is_current") == False)

    unchanged_existing = (
        current_existing
        .join(unchanged_keys, on=scd2_key, how="inner")
    )

    result = (
        historical
        .unionByName(closed_records)
        .unionByName(changed)
        .unionByName(new_records)
        .unionByName(unchanged_existing)
    )

    logger.info(
        f"SCD2 complete — "
        f"new: {new_records.count()}, "
        f"changed: {changed.count()}, "
        f"unchanged: {unchanged_keys.count()}"
    )

    return result