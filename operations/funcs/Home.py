from operations import app
import logging
@app.route("/")
def home():
    return 'OK', 200

@app.route('/health/liveness', methods=['GET'])
def liveness():
    return "OK", 200

@app.route('/health/readiness', methods=['GET'])
def readiness():
    return "OK", 200

@app.route('/health', methods=['GET'])
def health():
    return "OK", 200
