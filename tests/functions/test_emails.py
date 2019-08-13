import json
from unittest.mock import MagicMock

import pytest
from sendgrid.helpers.mail import Mail


from functions import emails
from tests.util import make_pubsub_event


def test_send_email(monkeypatch):
    """Test that the email sending function builds a message as expected."""
    sender = MagicMock()
    sg_client = MagicMock()
    sg_client.return_value = sender
    monkeypatch.setattr(emails, "_get_sg_client", sg_client)

    # Well-formed email
    email = {
        "to_emails": ["foo@bar.com", "bar@foo.org"],
        "subject": "test subject",
        "html_content": "test content",
    }
    event = make_pubsub_event(json.dumps(email))
    emails.send_email(event, None)
    args, _ = sender.send.call_args
    message = args[0]
    # A SendGrid message's string representation is a JSON blob
    # detailing its configuration.
    assert str(message) == str(Mail(from_email=emails.FROM_EMAIL, **email))

    # Malformed email
    del email["subject"]
    event = make_pubsub_event(json.dumps(email))
    with pytest.raises(AssertionError):
        emails.send_email(event, None)
