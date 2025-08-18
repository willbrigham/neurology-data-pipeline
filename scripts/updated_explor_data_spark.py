#!/usr/bin/env python3
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_unixtime

def build_spark(jar_path: str) -> SparkSession:
    return (
        SparkSession.builder
            .appName("NeuroFinder")
            .config("spark.jars", jar_path)  # e.g., libs/mssql-jdbc-12.10.1.jre11.jar
            .getOrCreate()
    )

def read_raw_json(spark: SparkSession, raw_path: str):
    df = (spark.read
          .option("multiLine", True)  # we saw multi-line JSON
          .json(raw_path))
    return df

def transform_tax(df):
    # keep a last_updated timestamp to support incrementals if needed
    df_tax = (
        df.select(
            col("number"),
            col("basic.first_name").alias("first_name"),
            col("basic.last_name").alias("last_name"),
            from_unixtime(col("last_updated_epoch").cast("bigint")/1000).alias("last_updated_ts"),
            explode("taxonomies").alias("taxonomy")
        )
        .select(
            col("number").cast("string"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("last_updated_ts"),
            col("taxonomy.code").alias("taxonomy_code").cast("string"),
            col("taxonomy.desc").alias("specialty").cast("string"),
            col("taxonomy.primary").alias("is_primary").cast("boolean")
        )
    )
    return df_tax

def transform_addresses(df):
    df_addr = (
        df.select(
            col("number"),
            from_unixtime(col("last_updated_epoch").cast("bigint")/1000).alias("last_updated_ts"),
            explode("addresses").alias("address")
        )
        .select(
            col("number").cast("string"),
            col("last_updated_ts"),
            col("address.address_1").alias("address_1").cast("string"),
            col("address.address_2").alias("address_2").cast("string"),
            col("address.city").alias("city").cast("string"),
            col("address.state").alias("state").cast("string"),
            col("address.postal_code").alias("postal_code").cast("string"),
            col("address.address_type").alias("address_type").cast("string"),
            col("address.address_purpose").alias("address_purpose").cast("string"),
        )
    )
    return df_addr

def write_staging(df_tax, df_addr, jdbc_url, props):
    # Overwrite staging deterministically but avoid drop/recreate churn:
    (df_tax.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(jdbc_url, "stg.Taxonomy", properties=props))

    (df_addr.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(jdbc_url, "stg.Addresses", properties=props))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw/neurologists_ma.json", help="Path to raw JSON")
    p.add_argument("--jdbc_url", default="jdbc:sqlserver://localhost:1433;databaseName=Neurology;encrypt=true;trustServerCertificate=true")
    p.add_argument("--jdbc_user", default="sa")
    p.add_argument("--jdbc_password", default="Brigham123$")
    p.add_argument("--jdbc_driver", default="com.microsoft.sqlserver.jdbc.SQLServerDriver")
    p.add_argument("--jdbc_jar", default="libs/mssql-jdbc-12.10.1.jre11.jar")
    args = p.parse_args()

    assert Path(args.raw).exists(), f"Raw file not found: {args.raw}"
    assert Path(args.jdbc_jar).exists(), f"JDBC jar not found: {args.jdbc_jar}"

    spark = build_spark(args.jdbc_jar)

    df_raw = read_raw_json(spark, args.raw)
    df_tax = transform_tax(df_raw)
    df_addr = transform_addresses(df_raw)

    props = {"user": args.jdbc_user, "password": args.jdbc_password, "driver": args.jdbc_driver}
    write_staging(df_tax, df_addr, args.jdbc_url, props)

    # quick sanity prints
    print("Taxonomy sample:")
    df_tax.show(5, truncate=False)
    print("Addresses sample:")
    df_addr.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()