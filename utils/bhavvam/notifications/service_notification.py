import os
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content

    # Load your SendGrid API key from environment variables
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")  # Store securely
SENDER_EMAIL = "your_verified_sender@example.com"  # Must be verified in SendGrid

def send_bill_upload_notification(user_email, bill_number, bill_link):
        """
        Sends an email notification to the user when a new bill is uploaded.

        Args:
            user_email (str): Recipient's email address.
            bill_number (str): Bill number/reference.
            bill_link (str): URL to view/download the bill.

        Returns:
            Response from SendGrid API.
        """
        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        
        subject = f"New Bill Uploaded: {bill_number}"
        content = f"""
        Hello,

        A new bill (Bill No: {bill_number}) has been uploaded.  
        You can view or download it using the link below:

        {bill_link}

        Best regards,  
        MINAKI Intell Team
        """

        mail = Mail(
            from_email=Email(SENDER_EMAIL),
            to_emails=To(user_email),
            subject=subject,
            plain_text_content=Content("text/plain", content),
        )

        try:
            response = sg.send(mail)
            print(f"Email sent to {user_email}. Status Code: {response.status_code}")
            return response
        except Exception as e:
            print(f"Failed to send email: {str(e)}")
            return None

    # Example usage:
    # send_bill_upload_notification("user@example.com", "BILL-12345", "https://drive.google.com/your-bill-link")
