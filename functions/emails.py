"""Functions for interacting with the Sendgrid API"""
import json

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import python_http_client.exceptions

from .settings import SENDGRID_API_KEY
from .util import BackgroundContext, extract_pubsub_data

FROM_EMAIL = "no-reply@cimac-network.org"

_sg = None


def _get_sg_client() -> SendGridAPIClient:
    global _sg
    if not _sg:
        _sg = SendGridAPIClient(SENDGRID_API_KEY)
    return _sg


def send_email(event: dict, context: BackgroundContext):
    """
    Send an email using the SendGrid API.
    Args:
        event - should consist at least of "to_emails", "subject", and "html_content"
                but can also include other properties supported by sendgrid json api.
    """
    print(f"got {event}")
    email = json.loads(extract_pubsub_data(event))

    assert "to_emails" in email
    assert "subject" in email
    assert "html_content" in email

    message = dict(
        Mail(  # converting standard args to what sendgrid is expecting
            from_email=FROM_EMAIL,
            to_emails=email.pop("to_emails"),
            subject=email.pop("subject"),
            html_content=email.pop("html_content"),
        ).get(),
        **email,  # including everything else as it is.
    )
    print(f"Sending email: {message}")

    sg = _get_sg_client()
    try:
        response = sg.send(message)
    except python_http_client.exceptions.HTTPError as e:
        print(f"Failed sending email: {e!r}")
        print(f"{e.__dict__!r}")
        raise e

    print(f"Email status code: {response.status_code}")
    print(f"Email response: {response} {response._headers}")
