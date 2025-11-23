# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Start Spark session
    spark = SparkSession.builder \
        .appName("RateStreamToDelta") \
        .getOrCreate()
    
    # Table name used for logging
    tableName = "streamingtable"
    
    # Define Delta Lake storage path
    deltaTablePath = f"Tables/{tableName}"
    
    # Create a streaming DataFrame using the rate source
    df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    
    # Write the streaming data to Delta
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("path", deltaTablePath) \
        .option("checkpointLocation", f"{deltaTablePath}/_checkpoint") \
        .start()
    
    # Keep the stream running
    query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
