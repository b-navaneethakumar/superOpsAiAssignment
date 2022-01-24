# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import socket
import credentials as creds

from TweetListener import TweetsListener


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def send_tweets(c_socket):
    # auth = OAuthHandler(consumer_key, consumer_secret)
    # auth.set_access_token(access_token, access_secret)
    #
    # twitter_stream = Stream(auth, TweetsListener(c_socket))
    # twitter_stream.filter(track = 'bose', languages=["en"])  # we are interested in this topic.
    twtr_stream = TweetsListener(
        creds.CONSUMER_KEY, creds.CONSUMER_SECRET,
        creds.ACCESS_TOKEN, creds.ACCESS_SECRET,
        csocket=c_socket
    )
    twtr_stream.filter(track=['bose'])

if __name__ == '__main__':
    TCP_IP = "0.0.0.0"
    TCP_PORT = 5599
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for the TCP connection...")
    conn, addr = s.accept()
    print("Connected successfully... Starting getting tweets.")
    send_tweets(conn)
