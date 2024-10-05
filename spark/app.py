import os
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("Spark + UCOSS + Delta") \
        .config("spark.jars.ivy", f"{os.getenv('HOME')}/.ivy2") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,"
                                       "io.unitycatalog:unitycatalog-spark_2.12:0.2.0-SNAPSHOT") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.spark_catalog.uri", "http://localhost:8080") \
        .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.unity.uri", "http://localhost:8080") \
        .getOrCreate()
    
    spark.catalog.setCurrentCatalog("unity")

    spark.table("default.numbers").show()


if __name__ == '__main__':
    main()
