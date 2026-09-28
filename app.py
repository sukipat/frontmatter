import hmac, hashlib, os
import json, sys
from flask import Flask, request, abort

app = Flask(__name__)
VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
APP_SECRET = os.environ["APP_SECRET"]

@app.get("/")
def health():
    return "ok", 200

@app.post("/webhook")
def receive():
    raw = request.get_data()
    sig = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(APP_SECRET.strip().encode(), raw, hashlib.sha256).hexdigest()
    valid = hmac.compare_digest(sig, expected)

    print(json.dumps({
        "valid_sig": valid,
        "sig_present": bool(sig),
        "body": raw.decode("utf-8", "replace")[:4000],
    }), file=sys.stderr, flush=True)

    if not valid:
        abort(403)
    return "ok", 200