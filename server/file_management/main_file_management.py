# from googleapiclient.discovery import build
# from googleapiclient.http import MediaFileUpload
# from google.oauth2 import service_account
# from datetime import datetime
# import os

# # Set up authentication
# SCOPES = ['https://www.googleapis.com/auth/drive']
# SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "service_account.json") # Replace with your service account JSON file

# credentials = service_account.Credentials.from_service_account_file(
#     SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# drive_service = build('drive', 'v3', credentials=credentials)

# def get_or_create_folder(folder_name, parent_folder_id=None):
#     """
#     Check if a folder exists in Google Drive; if not, create it.
#     Returns the folder ID.
#     """
#     query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
#     if parent_folder_id:
#         query += f" and '{parent_folder_id}' in parents"

#     results = drive_service.files().list(q=query, fields="files(id)").execute()
#     folders = results.get("files", [])

#     if folders:
#         return folders[0]['id']  # Return existing folder ID

#     # Create new folder
#     folder_metadata = {
#         'name': folder_name,
#         'mimeType': 'application/vnd.google-apps.folder',
#         'parents': [parent_folder_id] if parent_folder_id else []
#     }
#     folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
#     return folder['id']

# def upload_to_drive(file_path, endpoint, serial_number, function_date):
#     """
#     Uploads a file to a dynamically created folder structure based on the document date.
#     Example: "Intell/Orders/2025-Mar/ORD12345.pdf"
    
#     :param file_path: Path of the file to upload
#     :param endpoint: The main category (e.g., Orders, Invoices, etc.)
#     :param serial_number: Unique identifier for the file
#     :param function_date: Date associated with the document (YYYY-MM-DD)
#     :return: Shareable link of the uploaded file
#     """
#     # Ensure the master folder "Intell" exists
#     master_folder_id = get_or_create_folder("Intell")

#     # Ensure the endpoint folder (e.g., "Orders") exists
#     endpoint_folder_id = get_or_create_folder(endpoint, master_folder_id)

#     # Convert function_date to "YYYY-Mon" format
#     doc_date = datetime.strptime(function_date, "%Y-%m-%d")  # Convert string to date
#     month_folder_name = doc_date.strftime("%Y-%b")  # Format: "2025-Mar"

#     # Ensure the YYYY-Mon folder exists inside the endpoint folder
#     month_folder_id = get_or_create_folder(month_folder_name, endpoint_folder_id)

#     # Upload file
#     file_name = f"{serial_number}.pdf"  # Example: ORD12345.pdf
#     file_metadata = {
#         'name': file_name,
#         'parents': [month_folder_id]
#     }
#     media = MediaFileUpload(file_path, resumable=True)
#     file = drive_service.files().create(
#         body=file_metadata, media_body=media, fields='id').execute()
    
#     file_id = file.get('id')

#     # Make the file shareable
#     drive_service.permissions().create(
#         fileId=file_id, body={'type': 'anyone', 'role': 'reader'}
#     ).execute()

#     # Get shareable link
#     shareable_link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    
#     return shareable_link

# # Example Usage
# # file_path = "invoice.pdf"  # Change to your file path
# # endpoint = "Invoices"  # Change this to "Orders", "Shipments", etc.
# # serial_number = "MNSO/INV1469"  # Unique identifier for the file
# # function_date = "2025-03-30"  # Document date (YYYY-MM-DD)

# # link = upload_to_drive(file_path, endpoint, serial_number, function_date)
# # print("File uploaded. Shareable link:", link)
