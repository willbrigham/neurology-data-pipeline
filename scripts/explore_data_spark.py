from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, upper

# Start spark session
spark = (
    SparkSession.builder
        .appName("NeuroFinder")
        .config("spark.jars", "libs/mssql-jdbc-12.10.1.jre11.jar")  # make sure this path is correct
        .getOrCreate()
)

# Read in json files
json_paths = [
    "data/raw/neurologists_ma.json",
    "data/raw/neurologists_ri.json",
]

df_raw = spark.read.option("multiLine", True).json(json_paths)

# Select nested taxonomy fields
df_tax = (
    df_raw
      .select(
          col("number"),
          col("basic.first_name").alias("first_name"),
          col("basic.last_name").alias("last_name"),
          explode("taxonomies").alias("taxonomy")
      )
      .select(
          "number",
          "first_name",
          "last_name",
          col("taxonomy.code").alias("taxonomy_code"),
          col("taxonomy.desc").alias("specialty"),
          col("taxonomy.primary").alias("is_primary")
      )
      .dropDuplicates(["number", "taxonomy_code", "is_primary"])
)
# Select nested address fields
df_addr = (
    df_raw
      .select("number", explode("addresses").alias("address"))
      .select(
          "number",
          trim(col("address.address_1")).alias("address_1"),
          trim(col("address.address_2")).alias("address_2"),
          upper(trim(col("address.city"))).alias("city"),
          upper(trim(col("address.state"))).alias("state"),
          trim(col("address.postal_code")).alias("postal_code"),
          upper(trim(col("address.address_purpose"))).alias("address_purpose")
      )
      .dropDuplicates(["number", "address_1", "postal_code", "address_purpose"])
)
# Check the top 5 rows
print("taxonomy rows:", df_tax.count())
print("address  rows:", df_addr.count())
df_tax.show(5, truncate=False)
df_addr.show(5, truncate=False)

# Write to the processed folder
df_tax.write.mode("overwrite").json("data/processed/taxonomy_flattened.json")
df_addr.write.mode("overwrite").json("data/processed/address_flattened.json")

# Connect via jdbc to the staging tables
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=Neurology;encrypt=true;trustServerCertificate=true"
connection_properties = {
    "user": "sa",
    "password": "Brigham123$",  # tip: swap to an env var later
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

(
    df_tax.write
        .mode("overwrite")          # staging-friendly; switch to 'append' if incremental
        .option("truncate", "true") # preserves table/indices where supported
        .option("batchsize", "10000")
        .jdbc(url=jdbc_url, table="stg.taxonomy", properties=connection_properties)
)

(
    df_addr.write
        .mode("overwrite")
        .option("truncate", "true")
        .option("batchsize", "10000")
        .jdbc(url=jdbc_url, table="stg.address", properties=connection_properties)
)

spark.stop()