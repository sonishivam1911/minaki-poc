import cv2
import numpy as np
import os

def remove_watermark(image_path, output_dir, sku):
    """
    Removes watermark from the image and saves it in the corresponding SKU folder.
    
    Args:
        image_path (str): Path to the input image.
        output_dir (str): Directory where the processed images will be saved.
        sku (str): SKU identifier for the product.

    Returns:
        str: Path to the saved processed image.
    """
    try:
        # Load the image
        image = cv2.imread(image_path)
        if image is None:
            raise ValueError(f"Unable to load image: {image_path}")

        # Get image dimensions
        height, width, _ = image.shape

        # Define watermark regions (manually defined based on images provided)
        # Adjust these values based on the position of "Xuping" and the website watermark
        watermark_regions = [
            (0, 0, 300, 100),  # Top-left region for "Xuping"
            (width - 500, height - 50, width, height)  # Bottom-right region for website URL
        ]

        mask = np.zeros(image.shape[:2], dtype=np.uint8)  # Mask for inpainting

        # Create mask for watermark regions
        for x1, y1, x2, y2 in watermark_regions:
            mask[y1:y2, x1:x2] = 255

        # Inpaint to remove watermark
        inpainted_image = cv2.inpaint(image, mask, inpaintRadius=3, flags=cv2.INPAINT_TELEA)

        # Create output folder for SKU if it doesn't exist
        sku_folder = os.path.join(output_dir, sku)
        os.makedirs(sku_folder, exist_ok=True)

        # Save the processed image
        output_path = os.path.join(sku_folder, os.path.basename(image_path))
        cv2.imwrite(output_path, inpainted_image)

        return output_path

    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return None


def process_images(image_paths, output_dir, sku):
    """
    Processes a list of images for a particular SKU.

    Args:
        image_paths (list): List of image paths to process.
        output_dir (str): Directory to save processed images.
        sku (str): SKU identifier for the product.
    """
    for image_path in image_paths:
        processed_path = remove_watermark(image_path, output_dir, sku)
        if processed_path:
            print(f"Processed and saved: {processed_path}")
        else:
            print(f"Failed to process: {image_path}")


def find_watermark_coordinates(image_path):
    """
    Allows the user to interactively find the top-left and bottom-right coordinates of the watermark.

    Args:
        image_path (str): Path to the input image.

    Returns:
        dict: A dictionary with coordinates for the top-left and bottom-right watermarks.
    """
    try:
        # Load the image
        image = cv2.imread(image_path)
        if image is None:
            raise ValueError(f"Unable to load image: {image_path}")

        print("Draw a rectangle around the TOP-LEFT watermark and press ENTER or SPACE when done.")
        # Select the top-left watermark region
        top_left_box = cv2.selectROI("Select TOP-LEFT Watermark", image, showCrosshair=True)
        cv2.destroyWindow("Select TOP-LEFT Watermark")
        
        # Expand the box programmatically
        top_left_x, top_left_y, top_left_w, top_left_h = top_left_box
        top_left_coordinates = (
            max(0, top_left_x - 10),  # Expand left
            max(0, top_left_y - 10),  # Expand up
            min(image.shape[1], top_left_x + top_left_w + 20),  # Expand right
            min(image.shape[0], top_left_y + top_left_h + 20)  # Expand down
        )

        print("Draw a rectangle around the BOTTOM-RIGHT watermark and press ENTER or SPACE when done.")
        # Select the bottom-right watermark region
        bottom_right_box = cv2.selectROI("Select BOTTOM-RIGHT Watermark", image, showCrosshair=True)
        cv2.destroyWindow("Select BOTTOM-RIGHT Watermark")
        
        # Expand the box programmatically
        bottom_right_x, bottom_right_y, bottom_right_w, bottom_right_h = bottom_right_box
        bottom_right_coordinates = (
            max(0, bottom_right_x - 20),  # Expand left
            max(0, bottom_right_y - 10),  # Expand up
            min(image.shape[1], bottom_right_x + bottom_right_w + 50),  # Expand right
            min(image.shape[0], bottom_right_y + bottom_right_h + 20)  # Expand down
        )

        # Print coordinates
        print(f"Top-Left Watermark Coordinates: {top_left_coordinates}")
        print(f"Bottom-Right Watermark Coordinates: {bottom_right_coordinates}")

        return {
            "top_left": top_left_coordinates,
            "bottom_right": bottom_right_coordinates
        }
    except Exception as e:
        print(f"Error: {e}")
        return None
    

    # Example Usage
if __name__ == "__main__":
    image_path = "/Users/shivamsoni/Downloads/56deed4120.jpg"  # Replace with your image path
    watermark_coordinates = find_watermark_coordinates(image_path)
    print("Watermark Coordinates:", watermark_coordinates)