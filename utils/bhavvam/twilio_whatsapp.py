# Download the helper library from https://www.twilio.com/docs/python/install
import os
from twilio.rest import Client
import json
import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv


# Find your Account SID and Auth Token at twilio.com/console
# and set the environment variables. See http://twil.io/secure
account_sid = os.environ["TWILIO_ACCOUNT_SID"]
auth_token = os.environ["TWILIO_AUTH_TOKEN"]
client = Client(account_sid, auth_token)

message = client.messages.create(
    content_sid="HX93ec366250a5fd6fbfe9aad915fd42ab",
    to="whatsapp:+18551234567",
    from_="whatsapp:+15005550006",
    content_variables=json.dumps({"1": "Name"}),
    messaging_service_sid="MGXXXXXXXX",
)

print(message.body)