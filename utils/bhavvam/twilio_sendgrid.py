import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

message = Mail(
    from_email='info@minaki.co.in',
    to_emails='bhavsoni500@gmail.com',
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
try:
    sg = SendGridAPIClient('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')
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)
except Exception as e:
    print(e.message)