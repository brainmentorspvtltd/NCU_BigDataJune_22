from flask import Flask

app = Flask(__name__)

@app.route('/')
def helloFlask():
    return "<h1>Hello Using Flask...</h1>"

if __name__ == '__main__':
    app.run()