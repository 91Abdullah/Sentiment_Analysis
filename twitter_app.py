import socket
import sys
import requests
import requests_oauthlib
import json
import preprocessor

# Twitter app secret
ACCESS_TOKEN = 'Enter your key here'
ACCESS_SECRET = 'Enter your key here'
CONSUMER_KEY = 'Enter your key here'
CONSUMER_SECRET = 'Enter your key here'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


# Get the stream
def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    # query_data = [('language', 'en'), ('track', 'pakistan')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(https_resp, tcp_connection):
    for line in https_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print("-------------------------------------------------------")
            tcp_connection.send((tweet_text + '\n').encode('UTF-8'))
        except:
            e = sys.exc_info()[1]
            print("Error: %s " % e)


TCP_IP = 'localhost'
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Start getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
