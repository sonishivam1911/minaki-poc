import re
from config.settings import CATEGORY_MAPPING

def generate_skus(df, existing_skus):
    """Generate unique SKUs for products based on their categories."""
    
    serial_trackers = {prefix: 1 for prefix in CATEGORY_MAPPING.values()}

    for sku in existing_skus:
        match = re.match(r"([A-Z]+)(\d{5})", sku)
        if match:
            prefix, serial_number = match.groups()
            if prefix in serial_trackers:
                serial_trackers[prefix] = max(serial_trackers[prefix], int(serial_number) + 1)

    generated_skus = []

    for _, row in df.iterrows():
        category_prefix = row.get("category")
        vendor_code = row.get("vendor_code")

        if not vendor_code or not category_prefix:
            generated_skus.append(None)
            continue

        serial_number = serial_trackers[category_prefix]
        new_sku = f"{category_prefix}{str(serial_number).zfill(5)}"

        while new_sku in existing_skus:
            serial_trackers[category_prefix] += 1
            serial_number = serial_trackers[category_prefix]
            new_sku = f"{category_prefix}{str(serial_number).zfill(5)}"

        generated_skus.append(new_sku)
        existing_skus.add(new_sku)

        serial_trackers[category_prefix] += 1

    df["Generated SKU"] = generated_skus

    return df
