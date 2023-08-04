from databricks.connect import DatabricksSession as SparkSession


spark = SparkSession.builder.getOrCreate()

# Define the S3 location and schema location
s3_location = "s3://databricks-streaming-data/broker-office.ReplicantCallDataExport"
dbfs_location = "/replicant-call-data-export-job"

# Define the output table name
table_name = "qa.data_ingestion.replicant_call_data_export_raw_data"

# Ingest data from S3 location
# cloudFiles.schemaEvolutionMode - addNewColumns is default setting and is not required to be set
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "avro") \
    .option("cloudFiles.schemaLocation", dbfs_location) \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.useNotifications", "true") \
    .load(s3_location)

# Transform or process data (if needed)
# For example, you can perform some data processing or filtering here, aggregation, joining.

# Assuming you want to write to a Delta Lake table
# outputMode - append is default setting and is not required to be set
streaming_data = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", dbfs_location) \
    .toTable(table_name)