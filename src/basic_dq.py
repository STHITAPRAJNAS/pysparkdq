from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    DoubleType,
)
import yaml
from datetime import date, timedelta

spark = SparkSession.builder.appName("ComplexDataValidation").getOrCreate()


def load_config(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def execute_sql_rule(spark, sql_expression, variables):
    formatted_sql = sql_expression.format(**variables)
    return spark.sql(formatted_sql)


def compare_result(result_df, expected_result):
    column = expected_result["column"]
    operator = expected_result["operator"]
    value = expected_result["value"]

    if operator == ">":
        return result_df.select(col(column) > value).first()[0]
    elif operator == "<":
        return result_df.select(col(column) < value).first()[0]
    elif operator == "between":
        return result_df.select(
            (col(column) >= value[0]) & (col(column) <= value[1])
        ).first()[0]
    # Add more operators as needed


def validate_rules(spark, config, variables):
    results = []
    for table in config["tables"]:
        for rule in table["rules"]:
            result_df = execute_sql_rule(spark, rule["sql_expression"], variables)
            is_valid = compare_result(result_df, rule["expected_result"])

            results.append(
                {
                    "table_name": table["name"],
                    "rule_id": rule["rule_id"],
                    "sql_expression": rule["sql_expression"],
                    "expected_result": rule["expected_result"],
                    "is_valid": is_valid,
                }
            )

    return results


def store_results(results):
    results_df = spark.createDataFrame(results)
    results_df.write.format("delta").mode("overwrite").save(
        "/path/to/validation_results"
    )


if __name__ == "__main__":

    # Initialize Spark session
    spark = SparkSession.builder.appName("DataValidationTest").getOrCreate()

    # Define schema for the sales table
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("order_date", DateType(), False),
            StructField("product_category", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("revenue", DoubleType(), False),
        ]
    )

    # Generate test data
    def generate_test_data():
        data = []
        categories = ["Electronics", "Clothing", "Books", "Home"]
        start_date = date(2024, 1, 1)

        for i in range(1000):  # Generate 1000 sample records
            order_date = start_date + timedelta(days=i % 365)
            category = categories[i % len(categories)]
            quantity = (i % 10) + 1
            revenue = quantity * (100.00 + (i % 100))

            data.append((f"ORD-{i:04d}", order_date, category, quantity, revenue))

        return spark.createDataFrame(data, schema)

    # Create and register the test table
    test_df = generate_test_data()
    test_df.createOrReplaceTempView("sales_table")
    config = load_config("config/validation_rules.yaml")
    variables = {
        "start_date": "2024-01-01",
        "categories": "('Electronics', 'Clothing')",
    }

    validation_results = validate_rules(spark, config, variables)
    #store_results(validation_results)

    for result in validation_results:
        print(
            f"Rule {result['rule_id']} for table {result['table_name']}: {'Passed' if result['is_valid'] else 'Failed'}"
        )
