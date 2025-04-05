import os
import base64
import streamlit as st
import email
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
service_account_info = st.secrets["gcp_service_account"]
USER_EMAIL = os.getenv("GMAIL_EMAIL")  # Replace with your Gmail address


def authenticate_gmail():
    """Authenticate using a service account with domain-wide delegation."""
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES
    )
    delegated_credentials = credentials.with_subject(USER_EMAIL)
    
    # Build the Gmail API service
    service = build("gmail", "v1", credentials=delegated_credentials)
    return service

# Search for purchase order emails with PDFs
def search_purchase_order_emails(service):
    # query = ''
    query = 'from:ppus_designers@purplestylelabs.com subject:("Purchase Order no. :") has:attachment filename:pdf'
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    return messages

# Download PDF attachments
def download_pdf_attachments(service, messages):
    pdf_files = []
    
    for msg in messages:
        msg_id = msg['id']
        msg_data = service.users().messages().get(userId='me', id=msg_id).execute()
        payload = msg_data['payload']

        if 'parts' in payload:
            for part in payload['parts']:
                if part.get('filename', '').endswith('.pdf'):  # Check if it's a PDF file
                    attachment_id = part['body']['attachmentId']
                    attachment = service.users().messages().attachments().get(
                        userId='me', messageId=msg_id, id=attachment_id).execute()
                    
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    file_path = os.path.join("downloads", part['filename'])
                    
                    os.makedirs("downloads", exist_ok=True)  # Ensure download folder exists
                    with open(file_path, 'wb') as f:
                        f.write(file_data)
                    
                    pdf_files.append(file_path)
                    print(f"Downloaded: {file_path}")
    
    return pdf_files

# Main execution
def main():
    service = authenticate_gmail()
    messages = search_purchase_order_emails(service)
    
    if messages:
        print(f"Found {len(messages)} emails with Purchase Order PDFs.")
        pdf_files = download_pdf_attachments(service, messages)
        print("Downloaded PDFs:", pdf_files)
    else:
        print("No matching emails found.")

if __name__ == "__main__":
    main()
