import tweepy
import requests, json
from tweepy import StreamRule, StreamingClient

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAALQXewEAAAAASRclBe6%2FzJQReXljbbexYznSBTY%3DSWEo8ibQIJ95RX3r1HB6RhDnesCSDGD77ihO78ajml92VK2D8M"

RULES_URL = "https://api.twitter.com/2/tweets/search/stream/rules"
STREAM_URL = "https://api.twitter.com/2/tweets/search/stream"

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

def set_rules():
    sample_rules = [
        {"value" : "#dhoni lang:en", "tag" : "dhoni csk"},
    ]
    payload = {"add" : sample_rules}
    res = requests.post(RULES_URL, auth = bearerAuth, json=payload)

    # if res.status_code != 200 or res.status_code != 201:
    #     raise Exception("Cannot Set Rules...{} : {}".format(res.status_code, res.text))

    print(json.dumps(res.json()))


def get_stream():
    response = requests.get(STREAM_URL, auth=bearerAuth, stream=True)
    print("Status Code :",response.status_code)
    if response.status_code != 200:
        raise Exception("Cannot Get Stream...{} : {}".format(response.status_code, response.text))

    for lines in response.iter_lines():
        if lines:
            json_response = json.loads(lines)
            print(json.dumps(json_response, sort_keys=True, indent=4))

def main():
    print("***** Getting Rules....*****")
    rules = get_rules()
    print("***** Deleting Rules....*****")
    delete_all_rules(rules)
    print("***** Setting New Rules....*****")
    set_rules()
    print("***** Getting Stream....*****")
    get_stream()

main()