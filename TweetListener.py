import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
import socket
import json

consumer_key    = '<Enter API Key>'
consumer_secret = '<Enter API Key Secret>'
access_token    = '<Enter Access Token>'
access_secret   = '<Enter Access Token Secret>'

class TweetsListener(tweepy.Stream):

    def __init__(self,*args, csocket):
        super().__init__(*args)
        self.client_socket = csocket
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads( data )
            print( message['text'].encode('utf-8') )
            self.client_socket.send( message['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True