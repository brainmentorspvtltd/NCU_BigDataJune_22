import sys, os
from pyspark.sql import SparkSession
import threading
import pandas as pd
from datetime import datetime as dt
import glob
import time
from flask import *
from turbo_flask import Turbo
import random

app = Flask(__name__)
turbo = Turbo(app)

def startStreaming():
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

    # lines.writeStream.format("console").option("truncate",False).start()
    (lines
    .writeStream
    .format("csv")
    .option("checkpointLocation", "checkpoint/")
    .option("path", "output_dir/")
    .option("truncate",False)
    .outputMode("append")
    .start())

    spark.streams.awaitAnyTermination()
    spark.close()

def update_load():
    with app.app_context():
        while True:
            try:
                time.sleep(2)
                t = dt.now().time()
                print("Current Time is :",t)
                
                csv_files = glob.glob("output_dir/*.csv")
                print("Number of Files : ", len(csv_files))

                file = random.choice(csv_files)
                df = pd.read_csv(file, header=None)
                print("Data Inside CSV",df)

                tweet_text = df.values[0][0]

                turbo.push(turbo.replace(render_template('load_tweets.html', current_time=t, tweet_text=tweet_text), 'load'))

            except BaseException as ex:
                pass

@app.before_first_request
def before_first_request():
    threading.Thread(target=update_load).start()

@app.route('/')
def index():
    threading.Thread(target=startStreaming).start()
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)