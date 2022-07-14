from flask import *
import FetchingTweets
app = Flask(__name__)

@app.route('/')
def index():
    FetchingTweets.getTweets()
    return render_template('index.html')

if __name__ == '__main__':
    app.run()