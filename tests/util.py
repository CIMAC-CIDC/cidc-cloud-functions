import base64


def make_pubsub_event(data: str) -> dict:
    """Make pubsub event dictionary with base64-encoded data."""
    b64data = base64.encodebytes(bytes(data, "utf-8"))
    return {"data": b64data}

