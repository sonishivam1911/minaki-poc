from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
import os
import zipfile

# Set up authentication
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "service_account.json")  # Ensure this is in the same directory

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

def get_or_create_folder(folder_name, parent_folder_id=None):
    """
    Check if a folder exists in Google Drive; if not, create it.
    Returns the folder ID.
    """
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"

    results = drive_service.files().list(q=query, fields="files(id)").execute()
    folders = results.get("files", [])

    if folders:
        return folders[0]['id']  # Return existing folder ID

    # Create new folder
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id] if parent_folder_id else []
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
    return folder['id']

def upload_product_photos(photo_paths, brand, category, sku, photo_type="inventory_photo"):
    """
    Uploads product photos to a structured Google Drive folder.

    :param photo_paths: List of file paths to upload
    :param brand: Brand name
    :param category: Product category
    :param sku: SKU identifier
    :param photo_type: Either "inventory_photo" or "listing_photo"
    :return: Dictionary with folder link and uploaded photo links
    """
    master_folder_id = get_or_create_folder("Intell")
    items_folder_id = get_or_create_folder("items", master_folder_id)
    photo_type_folder_id = get_or_create_folder(photo_type, items_folder_id)
    brand_folder_id = get_or_create_folder(brand, photo_type_folder_id)
    category_folder_id = get_or_create_folder(category, brand_folder_id)
    sku_folder_id = get_or_create_folder(sku, category_folder_id)

    # Upload files and store their links
    photo_links = []
    for index, photo_path in enumerate(photo_paths, start=1):
        file_name = f"{sku}_{index}.jpg"  # Example: SKU1234_1.jpg, SKU1234_2.jpg
        file_metadata = {
            'name': file_name,
            'parents': [sku_folder_id]
        }
        media = MediaFileUpload(photo_path, resumable=True)
        file = drive_service.files().create(
            body=file_metadata, media_body=media, fields='id').execute()

        file_id = file.get('id')

        # Make file shareable
        drive_service.permissions().create(
            fileId=file_id, body={'type': 'anyone', 'role': 'reader'}
        ).execute()

        # Generate shareable link
        photo_links.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")

    # Generate SKU folder shareable link
    folder_link = f"https://drive.google.com/drive/folders/{sku_folder_id}?usp=sharing"

    return {
        "folder_link": folder_link,
        "photo_links": photo_links
    }

def extract_zip_and_upload(zip_path, brand, category, sku, photo_type="inventory_photo"):
    """
    Extracts a ZIP file and uploads the extracted images to Google Drive.

    :param zip_path: Path to the ZIP file.
    :param brand: Brand name.
    :param category: Product category.
    :param sku: SKU identifier.
    :param photo_type: Either "inventory_photo" or "listing_photo".
    :return: Dictionary with folder link and uploaded photo links.
    """
    extracted_folder = f"temp_{sku}"
    
    # Extract ZIP
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)

    # Get all extracted image paths
    photo_paths = [os.path.join(extracted_folder, file) for file in os.listdir(extracted_folder) if file.lower().endswith(('png', 'jpg', 'jpeg'))]

    # Upload images
    result = upload_product_photos(photo_paths, brand, category, sku, photo_type)

    # Cleanup: Remove extracted files after upload
    for file in photo_paths:
        os.remove(file)
    os.rmdir(extracted_folder)

    return result

# # Example Usage for Multiple Image Upload
# photo_paths = ["path/to/photo1.jpg", "path/to/photo2.jpg"]  # Local file paths
# brand = "Minaki"
# category = "Necklaces"
# sku = "MNSK123"
# photo_type = "inventory_photo"  # or "listing_photo"
# 
# result = upload_product_photos(photo_paths, brand, category, sku, photo_type)
# print("Folder Link:", result["folder_link"])
# print("Photo Links:", result["photo_links"])

# # Example Usage for ZIP Upload
# zip_path = "path/to/photos.zip"
# result_zip = extract_zip_and_upload(zip_path, brand, category, sku, photo_type)
# print("Folder Link:", result_zip["folder_link"])
# print("Photo Links:", result_zip["photo_links"])
