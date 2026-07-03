import pytest
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from my_project.tasks.core.base_gold_task import GoldTask


# ===========================
# CONCRETE IMPLEMENTATIONS FOR TESTING
# ===========================


class AppendGoldTask(GoldTask):
    """Concrete implementation using append load type"""

    load_type = "append"

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("name", F.upper(F.col("name")))


class Scd1GoldTask(GoldTask):
    """Concrete implementation using SCD1 load type"""

    load_type = "scd1"

    def transform(self, df: DataFrame) -> DataFrame:
        return df.filter(F.col("status") == "active")


class Scd2GoldTask(GoldTask):
    """Concrete implementation using SCD2 load type"""

    load_type = "scd2"
    scd2_key = "id"
    scd2_track_columns = ["name", "status"]

    def transform(self, df: DataFrame) -> DataFrame:
        return df


class OverwriteGoldTask(GoldTask):
    """Concrete implementation using overwrite load type"""

    load_type = "overwrite"

    def transform(self, df: DataFrame) -> DataFrame:
        return df.groupBy("status").count()


# ===========================
# BASE CLASS TESTS
# ===========================


class TestGoldTaskInterface:

    def test_cannot_instantiate_without_transform(self, spark, tmp_path):
        """GoldTask without transform() should raise NotImplementedError"""

        class IncompleteTask(GoldTask):
            pass

        task = IncompleteTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        df = spark.createDataFrame(
            [(1, "alice", "active")],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("status", StringType(), True),
                ]
            ),
        )
        with pytest.raises(NotImplementedError):
            task.transform(df)

    def test_default_load_type_is_overwrite(self, spark, tmp_path):
        """Default load_type should be overwrite"""
        task = OverwriteGoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        assert task.load_type == "overwrite"

    def test_input_path_stored_on_task(self, spark, tmp_path):
        """input_path should be stored as instance attribute"""
        task = AppendGoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        assert task.input_path == str(tmp_path / "input")

    def test_output_path_stored_on_task(self, spark, tmp_path):
        """output_path should be stored as instance attribute"""
        task = AppendGoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        assert task.output_path == str(tmp_path / "output")


class TestGoldTaskRead:

    @pytest.fixture(scope="class")
    def input_parquet(self, spark, tmp_path_factory):
        """Write a parquet file to read from"""
        path = str(tmp_path_factory.mktemp("input"))
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        spark.createDataFrame(
            [(1, "alice", "active"), (2, "bob", "inactive")], schema
        ).write.mode("overwrite").parquet(path)
        return path

    def test_read_returns_dataframe(self, spark, input_parquet, tmp_path):
        """read() should return a DataFrame"""
        task = AppendGoldTask(
            spark=spark, input_path=input_parquet, output_path=str(tmp_path / "output")
        )
        result = task.read()
        assert isinstance(result, DataFrame)

    def test_read_returns_correct_row_count(self, spark, input_parquet, tmp_path):
        """read() should return all rows from the parquet"""
        task = AppendGoldTask(
            spark=spark, input_path=input_parquet, output_path=str(tmp_path / "output")
        )
        result = task.read()
        assert result.count() == 2


class TestGoldTaskWrite:

    @pytest.fixture
    def sample_df(self, spark):
        """Sample DataFrame for write tests"""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        return spark.createDataFrame([(1, "ALICE", "active")], schema)

    def test_append_write_creates_output(self, spark, sample_df, tmp_path):
        """append load_type should write parquet output"""
        task = AppendGoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        task.write(sample_df)
        assert os.path.exists(str(tmp_path / "output"))

    def test_scd1_write_creates_output(self, spark, sample_df, tmp_path):
        """scd1 load_type should write parquet output"""
        task = Scd1GoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        task.write(sample_df)
        assert os.path.exists(str(tmp_path / "output"))

    def test_overwrite_write_creates_output(self, spark, sample_df, tmp_path):
        """overwrite load_type should write parquet output"""
        task = OverwriteGoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        task.write(sample_df)
        assert os.path.exists(str(tmp_path / "output"))

    def test_scd2_write_creates_output(self, spark, sample_df, tmp_path):
        """scd2 load_type should write parquet output"""
        task = Scd2GoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
            scd2_key="id",
            scd2_track_columns=["name", "status"],
        )
        task.write(sample_df)
        assert os.path.exists(str(tmp_path / "output"))

    def test_append_write_is_readable(self, spark, sample_df, tmp_path):
        """Output written by append task should be readable"""
        task = AppendGoldTask(
            spark=spark,
            input_path=str(tmp_path / "input"),
            output_path=str(tmp_path / "output"),
        )
        task.write(sample_df)
        result = spark.read.parquet(str(tmp_path / "output"))
        assert result.count() == 1


class TestGoldTaskRun:

    def test_run_executes_full_pipeline(self, spark, tmp_path):
        """run() should read, transform and write"""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        input_path = str(tmp_path / "input")
        output_path = str(tmp_path / "output")

        spark.createDataFrame(
            [(1, "alice", "active"), (2, "bob", "inactive")], schema
        ).write.parquet(input_path)

        task = Scd1GoldTask(spark=spark, input_path=input_path, output_path=output_path)
        task.run()

        result = spark.read.parquet(output_path)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "alice"

    def test_run_applies_transform(self, spark, tmp_path):
        """run() should apply the transform before writing"""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        input_path = str(tmp_path / "input")
        output_path = str(tmp_path / "output")

        spark.createDataFrame([(1, "alice", "active")], schema).write.parquet(
            input_path
        )

        task = AppendGoldTask(
            spark=spark, input_path=input_path, output_path=output_path
        )
        task.run()

        result = spark.read.parquet(output_path)
        assert result.collect()[0]["name"] == "ALICE"
