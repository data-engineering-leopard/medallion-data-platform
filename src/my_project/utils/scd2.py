import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

logger = logging.getLogger(__name__)

MAX_DATE = "9999-12-31 00:00:00"


def apply_scd2(
    spark: SparkSession,
    incoming_df: DataFrame,
    existing_df: DataFrame,
    scd2_key: str,
    track_columns: list,
    effective_from_ts: str = None,
    effective_from_column: str = None,
    effective_from_fallback_column: str = None
) -> dict:
    """
    Applies SCD2 logic to merge incoming data with existing data.

    Effective from priority order:
    1. Use effective_from_column value if not null (e.g. updated_date)
    2. Fall back to effective_from_fallback_column (e.g. created_date)
    3. Fall back to effective_from_ts if provided
    4. Fall back to current_timestamp()

    Records missing both date columns are quarantined.

    Returns a dict with two DataFrames:
        - valid: records that passed validation and were processed
        - quarantine: records missing required date columns
    """

    # ===========================
    # STEP 1: Separate quarantine records
    # Records missing both date columns cannot be processed
    # ===========================
    if effective_from_column and effective_from_fallback_column:
        quarantine_df = incoming_df.filter(
            F.col(effective_from_column).isNull() &
            F.col(effective_from_fallback_column).isNull()
        )
        valid_incoming_df = incoming_df.filter(
            F.col(effective_from_column).isNotNull() |
            F.col(effective_from_fallback_column).isNotNull()
        )

        if quarantine_df.count() > 0:
            logger.warning(
                f"Quarantining {quarantine_df.count()} records "
                f"missing both date columns"
            )
    else:
        quarantine_df = spark.createDataFrame([], incoming_df.schema)
        valid_incoming_df = incoming_df

    # ===========================
    # STEP 2: Determine effective_from for each record
    # ===========================
    if effective_from_column and effective_from_fallback_column:
        # Use updated_date if available, fall back to created_date
        effective_from = F.coalesce(
            F.to_timestamp(F.col(effective_from_column)),
            F.to_timestamp(F.col(effective_from_fallback_column))
        )
    elif effective_from_ts:
        # Use hardcoded timestamp from config
        effective_from = F.to_timestamp(F.lit(effective_from_ts))
    else:
        # Default to current time
        effective_from = F.current_timestamp()

    # ===========================
    # STEP 3: Add SCD2 metadata columns to valid incoming records
    # ===========================
    incoming_with_meta = (
        valid_incoming_df
        .withColumn("effective_from", effective_from)
        .withColumn(
            "effective_to",
            F.to_timestamp(F.lit(MAX_DATE))
        )
        .withColumn("is_current", F.lit(True).cast(BooleanType()))
    )

    # ===========================
    # STEP 4: Handle empty existing table
    # ===========================
    if existing_df.count() == 0:
        logger.info(
            f"No existing data — inserting "
            f"{incoming_with_meta.count()} new records"
        )
        return {
            "valid": incoming_with_meta,
            "quarantine": quarantine_df
        }

    # ===========================
    # STEP 5: Identify changed records
    # ===========================
    current_existing = existing_df.filter(F.col("is_current") == True)

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

    changed = joined.filter(change_condition).select("incoming.*")
    new_records = joined.filter(
        F.col(f"existing.{scd2_key}").isNull()
    ).select("incoming.*")

    unchanged_keys = joined.filter(
        ~change_condition
    ).select(F.col(f"incoming.{scd2_key}").alias(scd2_key))

    # ===========================
    # STEP 6: Close old versions of changed records
    # ===========================
    changed_keys = changed.select(scd2_key)

    closed_records = (
        current_existing
        .join(changed_keys, on=scd2_key, how="inner")
        .withColumn("effective_to", F.current_timestamp())
        .withColumn("is_current", F.lit(False).cast(BooleanType()))
    )

    # ===========================
    # STEP 7: Assemble the final DataFrame
    # ===========================
    historical = existing_df.filter(F.col("is_current") == False)

    unchanged_existing = (
        current_existing
        .join(unchanged_keys, on=scd2_key, how="inner")
    )

    valid_result = (
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
        f"unchanged: {unchanged_keys.count()}, "
        f"quarantined: {quarantine_df.count()}"
    )

    return {
        "valid": valid_result,
        "quarantine": quarantine_df
    }