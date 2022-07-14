import tweepy
import requests, json
from tweepy import StreamRule, StreamingClient
import socket

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAALQXewEAAAAASRclBe6%2FzJQReXljbbexYznSBTY%3DSWEo8ibQIJ95RX3r1HB6RhDnesCSDGD77ihO78ajml92VK2D8M"

RULES_URL = "https://api.twitter.com/2/tweets/search/stream/rules"
STREAM_URL = "https://api.twitter.com/2/tweets/search/stream"

class ShowTweet(StreamingClient):
    def on_tweet(self, tweet):
        try:
            global message
            message = tweet.text
            print(message)
            print("*" * 50)
            return True
        except BaseException as ex:
            print("Exception : ",ex)
        return True

def bearerAuth(req):
    req.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    req.headers["User-Agent"] = "v2FilteredStreamPython"
    return req

def get_rules():
    res = requests.get(RULES_URL, auth = bearerAuth)
    if res.status_code != 200:
        raise Exception("Cannot Get Rules...{} : {}".format(res.status_code, res.text))
    
    print(json.dumps(res.json()))
    return res.json()

def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None
    
    ids = list(map(lambda rule : rule["id"], rules["data"]))
    payload = {"delete" : {"ids" : ids}}
    res = requests.post(RULES_URL, auth = bearerAuth, json=payload)

    if res.status_code != 200:
        raise Exception("Cannot Delete Rules...{} : {}".format(res.status_code, res.text))

    print(json.dumps(res.json()))

def getTweets():
    obj = ShowTweet(BEARER_TOKEN)
    rules = get_rules()
    delete_all_rules(rules)

    rule = StreamRule(value = "ronaldo lang:en")
    obj.add_rules(rule)
    obj.filter()
