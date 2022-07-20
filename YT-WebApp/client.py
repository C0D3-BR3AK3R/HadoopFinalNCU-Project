import os, sys
from tracemalloc import start
from pyspark.sql import SparkSession
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
    HOST = 'localhost'
    PORT = 9999
    
    # Reading data from network
    
    lines = (spark
             .readStream
             .format("socket")
             .option("host", HOST)
             .option("port", PORT)
             .load())
    
    print("Is Streaming :",lines.isStreaming)

    lines.writeStream.format("console").option("truncate",False).start()
    # (lines
    # .writeStream
    # .format("csv")
    # .option("checkpointLocation", "checkpoint/")
    # .option("path", "output_dir/")
    # .option("truncate",False)
    # .outputMode("append")
    # .start())

    spark.streams.awaitAnyTermination()
    spark.close()
    
start_streaming()