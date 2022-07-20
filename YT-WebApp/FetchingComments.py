import socket
from googleapiclient.discovery import build

API_key = 'AIzaSyBlQUfsYPB9PqrlTUk-9HlEZOUiRg53LqQ'
video_ID = "5-eFLcCDNo8"

class FetchComments():
    
    def __init__(self, api_key, vid_id):

        resource = build('youtube', 'v3', developerKey=api_key)

        request = resource.commentThreads().list(part="snippet", videoId=vid_id, maxResults= 50, order="orderUnspecified")
        response = request.execute()

        items = response['items'][:]

        print("------------------------------------------------------------------------------------------------------")
        for item in items:
            item_info = item["snippet"]
            
            #the top level comment can have sub reply comments
            topLevelComment = item_info["topLevelComment"]
            comment_info = topLevelComment["snippet"]
            
            try:
                comment_text = comment_info['textDisplay'].encode('utf-8')
                print("Coment Text:" ,comment_text)
                clientSocket.send((comment_text+'\n').encode())
            except BaseException as ex:
                print('Issue in fetching comment.',ex)
            
            
            """ print("Comment By:", comment_info["authorDisplayName"].encode('utf-8'))
            print("Coment Text:" ,comment_info["textDisplay"].encode('utf-8'))
            print("Likes on Comment :", comment_info["likeCount"])
            print("Comment Date: ", comment_info['publishedAt'])
            print("================================\n") """
            
            
serverSocket = socket.socket()
HOST = "127.0.0.1"
PORT = 9999
serverSocket.bind((HOST, PORT))
print("Server is ready...Waiting for Client...")
serverSocket.listen(4)

clientSocket, address = serverSocket.accept()   # Handshaking
print("Client Arrived...")

obj = FetchComments(api_key=API_key, vid_id=video_ID)




    