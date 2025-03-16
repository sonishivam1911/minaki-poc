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
    content_sid="HXdde08b3177f7d0575d227a51ec1dfa49",
    to="whatsapp:+919810690118",
    from_="whatsapp:+15557179136",
    content_variables=json.dumps({"Customer Name": "Shivam",
                                  "Invoice_Number": "MN/INV0500",
                                  "Total_Amount": "₹7,200",
                                  "Store_Name": "*This is an example of programmatic messages capabilities of MINAKI Intell*",
                                  "Purchase_Date": "8 Mar 2025"}),
    messaging_service_sid="MG039514c4da7617fa5532629f358a1057"
)

print(message.body)