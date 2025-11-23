from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Start Spark session
    spark = SparkSession.builder \
        .appName("RateStreamToDelta") \
        .getOrCreate()
    
    # Table name used for logging
    tableName = "streamingtable"
    
    # Define Delta Lake storage path
    deltaTablePath = f"dbo/Tables/{tableName}"
    
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