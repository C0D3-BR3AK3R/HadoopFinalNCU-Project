import socket
from googleapiclient.discovery import build
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import NaiveBayesModel
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import DataPrep

API_key = 'AIzaSyBlQUfsYPB9PqrlTUk-9HlEZOUiRg53LqQ'

class FetchComments():
    
    def __init__(self, csocket):
        self.client_socket = csocket
        
    def on_data(self, vid_id):
        
        resource = build('youtube', 'v3', developerKey=API_key)
        
        request = resource.commentThreads().list(part='snippet', videoID=vid_id, maxResults= 5, order="orderUnspecified")
        response = request.execute()
        
        items = response['items'][:]
        
        for item in items:
            item_info = item['snippet']
            
            # Top level comment can have subcomments
            top_level_comment = item_info['topLeveComment']
            comment_info = top_level_comment['snippet']
            
            try: 
                comment_text = comment_info['textDisplay'].encode('utf-8').decode('utf-8', 'ignore')
                self.client_socket.send(comment_text)
                return True
            
            except BaseException as ex:
                print('Issue in fetching comments', ex)
                
            return True
        
        def on_error(self, status):
            print(status)
            return True
