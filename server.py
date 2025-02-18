"""HTTP Server for MiniPG
"""
import atexit
import json
from flask import Flask, request
from engine import MiniPGEngine

app = Flask(__name__)

with app.app_context():
    global engine
    engine = MiniPGEngine()

@app.route('/query', methods=['POST'])
def query():
    query = request.get_json()["query"]
    try:
        (msg, result) = engine.run_query(query)
        print(msg, result, flush=True)
        return result
    except Exception as e:
        return str(e)

@app.route('/engine-status', methods=['GET'])
def engine_status():
    return json.dumps(engine.__dict__, default=str)

# Register the shutdown function to be called when the app exits
atexit.register(lambda: engine.__exit__(None, None, None))
if __name__ == '__main__':
    app.run(port=5433)
