from .grant_permissions import permissions_worker
from .util import BackgroundContext, extract_pubsub_data


def worker(event: dict, context: BackgroundContext):
    """For use in parallelizing cloud function code"""
    try:
        # this returns the str, then convert it to a dict
        # uses event["data"] and then assumes format, so will error if no/malformatted data
        data: str = extract_pubsub_data(event)
        data: dict = dict(eval(data))
    except:
        raise

    else:
        fn = data.pop("_fn", "")
        if not fn:
            raise Exception(
                f"fn must be provided to pass remaining kwargs, you provided: {data}"
            )

        # ---- Function handling ----
        elif fn == "permissions_worker":
            permissions_worker(**data)
