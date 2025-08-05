from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NeuroFinder") \
    .config("spark.jars", "libs/mssql-jdbc-12.10.1.jre11.jar") \
    .getOrCreate()

# Load the JSON file
df = spark.read.option("multiLine", True).json("data/raw/neurologists_ma.json")
df.show(5, truncate=False)

# Print schema to understand the structure
df.printSchema()

# Show a few rows
df.select("number", "basic.first_name", "basic.last_name", "enumeration_type").show(5, truncate=False)

# Flatten taxonomies (if you want to analyze specialties)
df_tax = df.select(
    "number",
    "basic.first_name",
    "basic.last_name",
    explode("taxonomies").alias("taxonomy")
).select(
    "number",
    "first_name",
    "last_name",
    col("taxonomy.code").alias("taxonomy_code"),
    col("taxonomy.desc").alias("specialty"),
    col("taxonomy.primary").alias("is_primary")
)

df_tax.show(10, truncate=False)

# Flatten addresses (optional)
df_addr = df.select(
    "number",
    explode("addresses").alias("address")
).select(
    "number",
    col("address.address_1"),
    col("address.address_2"),
    col("address.city"),
    col("address.state"),
    col("address.postal_code"),
    col("address.address_type"),
    col("address.address_purpose")
)

df_addr.show(10, truncate=False)

# Write the output
df_tax.write.mode("overwrite").json("data/processed/taxonomy_flattened.json")
df_addr.write.mode("overwrite").json("data/processed/address_flattened.json")

jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=Neurology;encrypt=true;trustServerCertificate=true"
connection_properties = {
    "user": "sa",
    "password": "Brigham123$",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Create the database if needed (manually in SSMS or via PySpark + SQL later)

# Write taxonomy table
df_tax.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table="Taxonomy", properties=connection_properties)

# Write address table
df_addr.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table="Addresses", properties=connection_properties)