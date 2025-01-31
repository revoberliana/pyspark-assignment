from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# PostgreSQL Configuration
postgres_host = "dataeng-postgres"
postgres_port = "5432"  # Default PostgreSQL port
postgres_dw_db = "warehouse"
postgres_user = "user"
postgres_password = "password"

# Initialize Spark Session with correct path format for the jar file
spark = SparkSession.builder \
    .appName("RetailDataProcessing") \
    .config("spark.jars", "file:///d/dibimbing/dibimbing/dibimbing_spark_airflow/postgresql.jar") \
    .getOrCreate()

# Database connection properties
jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_dw_db}"
properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

# Read the 'retail' table
retail_df = spark.read.jdbc(url=jdbc_url, table="retail", properties=properties)

# Show sample data
retail_df.show()
