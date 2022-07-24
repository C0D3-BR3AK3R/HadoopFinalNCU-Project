from flask import Flask, render_template, url_for, request
from turbo_flask import Turbo
import threading
import time

from googleapiclient.discovery import build
from pyspark.ml.classification import NaiveBayesModel

import DataPrep
import pandas as pd

API_key = 'AIzaSyBlQUfsYPB9PqrlTUk-9HlEZOUiRg53LqQ'

def fetch_comments(vid_id):
    
    dp = DataPrep.data_prepper()
    spam_model = NaiveBayesModel.load('model/nbSpamFilter.model')
    
    resource = build('youtube', 'v3', developerKey=API_key)

    request = resource.commentThreads().list(part="snippet", videoId=vid_id, maxResults= 5, order="orderUnspecified")
    response = request.execute()

    items = response['items'][:]
    
    for item in items:
        item_info = item["snippet"]
        
        #the top level comment can have sub reply comments
        top_level_comment = item_info["topLevelComment"]
        comment_info = top_level_comment["snippet"]
        
        try:
            comment_text = comment_info['textDisplay'].encode('utf-8').decode('utf-8', 'ignore')
            pred = spam_model.predict(dp.clean_data(comment_text))
            
            final_pred = 0
            
            if(int(pred) == 0):
                final_pred = 1
                
            print('-'*75)
            print("Comment Text:" ,comment_text)
            print("Spam / Ham:", final_pred)
    
            yield (comment_text, final_pred)
            
        except BaseException as ex:
            print('Issue in fetching comment.',ex)
            
""" def update_load():
    
    with app.app_context():
        while True:
            time.sleep(5)
            turbo.push(turbo.replace(render_template('index.html'), 'load'))
 """

app = Flask(__name__)

# turbo = Turbo(app)

@app.route('/')
def index():
    return render_template('index.html')
    
@app.route('/', methods=['POST', 'GET'])
def my_form_post():
    
    if request.method == 'POST':
        vid_id = request.form['URL']
    
    return html_table(vid_id)

""" @app.before_request
def beforet_request():
    threading.Thread(target=update_load).start() """

@app.route('/', methods=['POST', 'GET'])
def html_table(video):
    
    comm_data = [i for i in fetch_comments(vid_id=video)]
    df = pd.DataFrame(comm_data, columns=['Comments', 'Spam / Ham'])    
    
    return render_template('commentStream.html', data = [df.to_html(classes='fl-table', header='true')])

if __name__ == '__main__':
    app.run(debug=True)