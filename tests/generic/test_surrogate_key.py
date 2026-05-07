from my_project.utils.surrogate_key import add_surrogate_key

class TestAddSurrogateKey:

    def test_surrogate_key_column_added(self, spark, customer_schema):
        """Surrogate key column should be added to the DataFrame"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK")]
        df = spark.createDataFrame(data, customer_schema)
        result = add_surrogate_key(df, "customer_key")
        assert "customer_key" in result.columns

    def test_surrogate_key_is_long_type(self, spark, customer_schema):
        """Surrogate key should be of LongType"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK")]
        df = spark.createDataFrame(data, customer_schema)
        result = add_surrogate_key(df, "customer_key")
        key_type = dict(result.dtypes)["customer_key"]
        assert key_type == "bigint"

    def test_surrogate_key_is_unique(self, spark, customer_schema):
        """Every row should have a unique surrogate key"""
        data = [
            (1, "ALICE", "alice@email.com","active", "UK"),
            (2, "BOB", "bob@email.com","active", "UK"),
            (3, "CHARLIE", "charlie@email.com","active", "UK")
        ]
        df = spark.createDataFrame(data, customer_schema)
        result = add_surrogate_key(df, "customer_key")
        total = result.count()
        distinct = result.select("customer_key").distinct().count()
        assert total == distinct

    def test_surrogate_key_is_not_null(self, spark, customer_schema):
        """Surrogate key should never be null"""
        data = [
            (1, "ALICE", "alice@email.com","active", "UK"),
            (2, "BOB", "bob@email.com","active", "UK")
        ]
        df = spark.createDataFrame(data, customer_schema)
        result = add_surrogate_key(df, "customer_key")
        null_count = result.filter(
            result["customer_key"].isNull()
        ).count()
        assert null_count == 0

    def test_surrogate_key_is_first_column(self, spark, customer_schema):
        """Surrogate key should be the first column in the DataFrame"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK")]
        df = spark.createDataFrame(data, customer_schema)
        result = add_surrogate_key(df, "customer_key")
        assert result.columns[0] == "customer_key"

    def test_existing_columns_preserved(self, spark, customer_schema):
        """All existing columns should still be present"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK")]
        df = spark.createDataFrame(data, customer_schema)
        result = add_surrogate_key(df, "customer_key")
        assert "id" in result.columns
        assert "name" in result.columns
        assert "email" in result.columns