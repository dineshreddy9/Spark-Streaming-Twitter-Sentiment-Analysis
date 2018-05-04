import json 
import tweepy
import socket
import sys
import re
import requests

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#'+sys.argv[1]


TCP_IP = 'localhost'
TCP_PORT = 9001


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
#conn, addr = s.accept()

def getLoc(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&key=AIzaSyCycfZdWxulqeK8aatreOFFIvYVF5ucCgM"
    r = requests.get(url)
    # extracting data in json format
    data = r.json()
    if data["results"] is not None and len(data["results"]) > 0:
        return str(data["results"][0]["geometry"]["location"]["lat"])+"::::"+str(data["results"][0]["geometry"]["location"]["lng"])
    else:
        return None
 

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, status):
        out = json.loads(status)
        #print(out["text"])
        rawlocation = out["user"]["location"]
        if hashtag in out["text"]:
          print(out["text"])
          if rawlocation is not None:
             cleaned  = ' '.join(re.sub("(@+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", out["text"]).split()) + '\n'
             jsonob = { "cleaned":cleaned}
             print(jsonob["cleaned"])
             loc = getLoc(rawlocation)
             jsonob["loc"] = loc
             if loc is not None:
                conn, addr = s.accept()
                djsonob=json.dumps(jsonob)
                conn.send(djsonob.encode('utf-8'))
                conn.close()
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])


