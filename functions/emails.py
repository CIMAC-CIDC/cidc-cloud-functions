"""Functions for interacting with the Sendgrid API"""
import json

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

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
    """Send an email using the SendGrid API."""
    email = json.loads(extract_pubsub_data(event))

    # Validate that the email blob has the expected structure
    assert "to_emails" in email
    assert "subject" in email
    assert "html_content" in email
    assert len(email) == 3  # no extra keys

    message = Mail(from_email=FROM_EMAIL, **email)

    print(f"Sending email: {email}")

    sg = _get_sg_client()
    response = sg.send(message)

    print(f"Email status code: {response.status_code}")
    print(f"Email response body: {response.body}")
