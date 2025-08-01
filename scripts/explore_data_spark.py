from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NeuroFinder") \
    .getOrCreate()

# Load the JSON file
df = spark.read.json("data/raw/neurologists_ma.json")

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
    col("address.address_purpose"),
    col("address.state"),
    col("address.city"),
    col("address.postal_code")
)

df_addr.show(10, truncate=False)