import base64
from functools import wraps

from flask import Flask


def make_pubsub_event(data: str) -> dict:
    """Make pubsub event dictionary with base64-encoded data."""
    b64data = base64.encodebytes(bytes(data, "utf-8"))
    return {"data": b64data}


def with_app_context(f):
    """Run `f` inside a default Flask app context"""

    @wraps(f)
    def wrapped(*args, **kwargs):
        app = Flask("test-app")
        with app.app_context():
            return f(*args, **kwargs)

    return wrapped
