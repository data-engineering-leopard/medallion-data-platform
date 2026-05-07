import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StructType, StructField, TimestampType
from my_project.utils.logger import get_logger

logger = get_logger(__name__)

MAX_DATE = "9999-12-31 00:00:00"


def separate_quarantine_records(
    df: DataFrame,
    effective_from_column: str,
    effective_from_fallback_column: str
) -> dict:
    """
    Separates incoming records into valid and quarantine.

    Records missing both date columns cannot have an effective_from
    date assigned and are quarantined.

    Returns a dict with:
        - valid: records with at least one date column present
        - quarantine: records missing both date columns
    """
    if effective_from_column and effective_from_fallback_column:
        quarantine_df = df.filter(
            F.col(effective_from_column).isNull() &
            F.col(effective_from_fallback_column).isNull()
        )
        valid_df = df.filter(
            F.col(effective_from_column).isNotNull() |
            F.col(effective_from_fallback_column).isNotNull()
        )

        if quarantine_df.count() > 0:
            logger.warning(
                f"Quarantining {quarantine_df.count()} records "
                f"missing both date columns"
            )
    else:
        quarantine_df = df.filter(F.lit(False))
        valid_df = df

    return {
        "valid": valid_df,
        "quarantine": quarantine_df
    }


def resolve_effective_from(
    df: DataFrame,
    effective_from_column: str = None,
    effective_from_fallback_column: str = None,
    effective_from_ts: str = None
) -> DataFrame:
    """
    Adds SCD2 metadata columns to a DataFrame.

    Effective from priority order:
    1. effective_from_column (e.g. updated_date) if not null
    2. effective_from_fallback_column (e.g. created_date)
    3. effective_from_ts if provided
    4. current_timestamp() as final fallback

    Also adds:
        - effective_to: set to MAX_DATE (9999-12-31)
        - is_current: set to True

    Returns DataFrame with three new SCD2 metadata columns added.
    """
    if effective_from_column and effective_from_fallback_column:
        effective_from = F.coalesce(
            F.to_timestamp(F.col(effective_from_column)),
            F.to_timestamp(F.col(effective_from_fallback_column))
        )
    elif effective_from_ts:
        effective_from = F.to_timestamp(F.lit(effective_from_ts))
    else:
        effective_from = F.current_timestamp()

    return (
        df
        .withColumn("effective_from", effective_from)
        .withColumn("effective_to", F.to_timestamp(F.lit(MAX_DATE)))
        .withColumn("is_current", F.lit(True).cast(BooleanType()))
    )


def merge_scd2(
    incoming_df: DataFrame,
    existing_df: DataFrame,
    scd2_key: str,
    track_columns: list
) -> DataFrame:
    """
    Performs the core SCD2 merge between incoming and existing data.

    Incoming DataFrame must already have SCD2 metadata columns added
    via resolve_effective_from() before calling this function.

    For each incoming record:
    - If brand new → insert as current
    - If changed tracked columns → close old version, insert new
    - If unchanged → keep existing, discard incoming duplicate

    Returns the full merged DataFrame including all historical versions.
    """
    # Handle empty existing table
    if existing_df.count() == 0:
        logger.info(
            f"No existing data — inserting "
            f"{incoming_df.count()} new records"
        )
        return incoming_df

    # Get only current records from existing
    current_existing = existing_df.filter(F.col("is_current") == True)

    # Build condition to detect changes in any tracked column
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

    incoming_aliased = incoming_df.alias("incoming")
    existing_aliased = current_existing.alias("existing")

    joined = incoming_aliased.join(
        existing_aliased,
        on=F.col(f"incoming.{scd2_key}") == F.col(f"existing.{scd2_key}"),
        how="left"
    )

    # Records where tracked columns have changed
    changed = joined.filter(change_condition).select("incoming.*")

    # Brand new records with no match in existing
    new_records = joined.filter(
        F.col(f"existing.{scd2_key}").isNull()
    ).select("incoming.*")

    # Records where nothing changed — keep existing version
    unchanged_keys = joined.filter(
        ~change_condition
    ).select(F.col(f"incoming.{scd2_key}").alias(scd2_key))

    # Close old versions of changed records
    changed_keys = changed.select(scd2_key)
    closed_records = (
        current_existing
        .join(changed_keys, on=scd2_key, how="inner")
        .withColumn("effective_to", F.current_timestamp())
        .withColumn("is_current", F.lit(False).cast(BooleanType()))
    )

    # Assemble final result
    historical = existing_df.filter(F.col("is_current") == False)
    unchanged_existing = current_existing.join(
        unchanged_keys, on=scd2_key, how="inner"
    )

    result = (
        historical
        .unionByName(closed_records)
        .unionByName(changed)
        .unionByName(new_records)
        .unionByName(unchanged_existing)
    )

    logger.info(
        f"SCD2 merge complete — "
        f"new: {new_records.count()}, "
        f"changed: {changed.count()}, "
        f"unchanged: {unchanged_keys.count()}"
    )

    return result


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
    Orchestrates the full SCD2 process.

    Delegates to three focused functions:
    1. separate_quarantine_records — splits valid vs quarantine
    2. resolve_effective_from — adds SCD2 metadata columns
    3. merge_scd2 — performs the actual merge

    Returns a dict with:
        - valid: processed DataFrame with full SCD2 history
        - quarantine: records missing required date columns
    """
    # Step 1 — separate quarantine records
    separated = separate_quarantine_records(
        df=incoming_df,
        effective_from_column=effective_from_column,
        effective_from_fallback_column=effective_from_fallback_column
    )

    valid_incoming = separated["valid"]
    quarantine_df = separated["quarantine"]

    # Step 2 — resolve effective_from and add SCD2 metadata
    incoming_with_meta = resolve_effective_from(
        df=valid_incoming,
        effective_from_column=effective_from_column,
        effective_from_fallback_column=effective_from_fallback_column,
        effective_from_ts=effective_from_ts
    )

    # Step 3 — merge with existing data
    valid_result = merge_scd2(
        incoming_df=incoming_with_meta,
        existing_df=existing_df,
        scd2_key=scd2_key,
        track_columns=track_columns
    )

    return {
        "valid": valid_result,
        "quarantine": quarantine_df
    }