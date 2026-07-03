import glob
import os

from pyspark.sql import DataFrame, SparkSession

from my_project.utils.logger import get_logger
from my_project.utils.scd2 import apply_scd2

logger = get_logger(__name__)


class GoldTask:
    """
    Base class for all Gold layer tasks.

    Defines the standard interface for reading, transforming
    and writing Gold tables. Subclasses must implement transform()
    and set load_type as a class attribute.

    Supported load types:
        - overwrite: overwrites the entire output table each run
        - scd1:      overwrites current record with latest version
        - scd2:      keeps full history of changes
        - append:    appends new records without removing existing ones

    Usage:
        class DimCustomersTask(GoldTask):
            load_type = "scd2"
            scd2_key = "customer_id"
            scd2_track_columns = ["email", "status", "country"]

            def transform(self, df: DataFrame) -> DataFrame:
                return add_surrogate_key(df, "customer_key")
    """

    load_type: str = "overwrite"
    scd2_key: str = None
    scd2_track_columns: list = None

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        scd2_key: str = None,
        scd2_track_columns: list = None,
    ):
        self.spark = spark
        self.input_path = input_path
        self.output_path = output_path

        # Allow scd2 config to be passed at instantiation
        # or defined as class attributes
        if scd2_key:
            self.scd2_key = scd2_key
        if scd2_track_columns:
            self.scd2_track_columns = scd2_track_columns

        logger.info(
            f"{self.__class__.__name__} initialised — " f"load_type={self.load_type}"
        )

    def read(self) -> DataFrame:
        """
        Reads the input parquet from input_path.
        Override in subclass if multiple inputs are needed.

        Returns:
            DataFrame read from input_path
        """
        logger.info(f"{self.__class__.__name__} reading from: {self.input_path}")
        return self.spark.read.parquet(self.input_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applies the Gold transformation to the input DataFrame.
        Must be implemented by every subclass.

        Args:
            df: Input DataFrame from read()

        Returns:
            Transformed DataFrame ready for write()

        Raises:
            NotImplementedError: If subclass does not implement this method
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement transform()"
        )

    def write(self, df: DataFrame) -> None:
        """
        Writes the transformed DataFrame to output_path.
        Behaviour is determined by load_type:

        - overwrite: full overwrite each run
        - scd1:      full overwrite (same as overwrite, alias for clarity)
        - append:    appends without removing existing records
        - scd2:      merges with full history tracking via apply_scd2()

        Args:
            df: Transformed DataFrame from transform()
        """
        logger.info(
            f"{self.__class__.__name__} writing — "
            f"load_type={self.load_type}, output={self.output_path}"
        )

        if self.load_type in ("overwrite", "scd1"):
            df.write.mode("overwrite").parquet(self.output_path)

        elif self.load_type == "append":
            df.write.mode("append").parquet(self.output_path)

        elif self.load_type == "scd2":
            if not self.scd2_key:
                raise ValueError(
                    f"{self.__class__.__name__} has load_type='scd2' "
                    f"but scd2_key is not set"
                )
            if not self.scd2_track_columns:
                raise ValueError(
                    f"{self.__class__.__name__} has load_type='scd2' "
                    f"but scd2_track_columns is not set"
                )

            # Load existing data if present
            parquet_files = glob.glob(f"{self.output_path}/*.parquet")
            has_existing = os.path.exists(self.output_path) and len(parquet_files) > 0

            if has_existing:
                existing_df = self.spark.read.parquet(self.output_path)
                existing_df = existing_df.cache()
                existing_df.count()
            else:
                existing_df = self.spark.createDataFrame([], df.schema)

            result = apply_scd2(
                spark=self.spark,
                incoming_df=df,
                existing_df=existing_df,
                scd2_key=self.scd2_key,
                track_columns=self.scd2_track_columns,
            )

            result["valid"].write.mode("overwrite").parquet(self.output_path)

        else:
            raise ValueError(
                f"{self.__class__.__name__} has unknown load_type "
                f"'{self.load_type}'. "
                f"Must be one of: overwrite, scd1, append, scd2"
            )

        logger.info(f"{self.__class__.__name__} write complete")

    def run(self) -> None:
        """
        Orchestrates the full Gold task:
        1. read()      — loads data from input_path
        2. transform() — applies business logic
        3. write()     — writes to output_path based on load_type

        In Databricks this is the method called by the job entry point.
        """
        logger.info(f"Starting {self.__class__.__name__}")
        df = self.read()
        df = self.transform(df)
        self.write(df)
        logger.info(f"Completed {self.__class__.__name__}")
