import os
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("Spark + UCOSS + Delta") \
        .config("spark.jars.ivy", f"{os.getenv('HOME')}/.ivy2") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.6,io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.unity.uri", "http://localhost:8080") \
        .config("spark.sql.catalog.unity.token", "") \
        .config("spark.sql.defaultCatalog", "unity") \
        .getOrCreate()

    # read in user_countries table
    spark.table("unity.default.user_countries").show()

    # create schema in unity catalog
    spark.sql("DROP SCHEMA IF EXISTS unity.customers CASCADE")
    spark.sql("CREATE SCHEMA IF NOT EXISTS unity.customers")

    print("created schema")

    # create new table
    spark.sql("""CREATE OR REPLACE TABLE
                unity.customers.user_countries_three (first_name string, age long, country string) 
                USING delta 
                LOCATION '/tmp/customers/user_countries_three';""")

    spark.sql("SHOW TABLES in unity.customers").show()

    # write to table using table location
    spark.table("unity.default.user_countries").write.format("delta").mode("overwrite").option("path","/tmp/customers/user_countries_three").saveAsTable("unity.customers.user_countries_three")

    spark.sql("SHOW TABLES in unity.customers").show()

    print("read new table")
    # read new table
    spark.table("unity.customers.user_countries_three").show()

    




if __name__ == '__main__':
    main()
