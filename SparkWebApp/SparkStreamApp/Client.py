import os, sys
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Stream Twitter").getOrCreate()
HOST = "localhost"
PORT = 9999

# read bytes from network
lines = (spark
        .readStream
        .format("socket")
        .option("host",HOST)
        .option("port",PORT)
        .load())

print("Is Streaming :",lines.isStreaming)

lines.writeStream.format("console").option("truncate",False).start()

spark.streams.awaitAnyTermination()
spark.close()