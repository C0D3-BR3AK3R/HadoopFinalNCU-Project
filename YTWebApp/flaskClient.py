import os, sys

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import threading
import pandas as pd
from datetime import datetime as dt

import glob
import time
from flask import Flask
from flask import render_template
from turbo_flask import Turbo
import random

app  = Flask(__name__)
turbo = Turbo(app)

def start_streaming():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    print("Spark Client Starte...")
    
    spark = SparkSession.builder.appName('YT Comment Streamer').getOrCreate()
    
    HOST = '127.0.0.1'
    PORT = 9999
    
    # Reading data from network
    
    lines = (spark
             .readStream
             .format("socket")
             .option("host", HOST)
             .option("port", PORT)
             .load())
    
    print("Is Streaming :",lines.isStreaming)

    lines.writeStream.format("console").option("truncate",False).start()            # Most important line for streaming the comments
    
    (lines
    .writeStream
    .format("csv")
    .option('format', 'append')
    .option("checkpointLocation", "checkpoint/")
    .option("path", "output_dir/")
    .option("truncate",False)
    .outputMode('append')
    .start())
    
    allfiles =  spark.read.option("header","false").csv("output_dir/part-*.csv")
    allfiles.coalesce(1).write.format("csv").option("header", "false").save("/output_dir/single_csv_file/")
    

    spark.streams.awaitAnyTermination()
    spark.close()

def update_load():
    print("Loading Data...")
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
    return render_template('index.html')

@app.route('/', methods=['POST', 'GET'])
def my_form_post():
    print("Server Started...")
    threading.Thread(target=start_streaming).start()
    start_streaming()
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)