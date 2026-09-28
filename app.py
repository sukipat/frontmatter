import hmac, hashlib, os
from flask import Flask, request, abort

app = Flask(__name__)
VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
APP_SECRET = os.environ["APP_SECRET"]

@app.get("/")
def health():
    return "ok", 200

@app.get("/webhook")
def verify():
    if (request.args.get("hub.mode") == "subscribe"
            and request.args.get("hub.verify_token") == VERIFY_TOKEN):
        return request.args["hub.challenge"], 200
    abort(403)

@app.post("/webhook")
def receive():
    sig = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(APP_SECRET.encode(), request.get_data(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        abort(403)
    print(request.get_json(silent=True))  # shows up in Vercel logs
    return "ok", 200
