import pdfplumber
import re
import json

def extract_taj_rtv_details(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    
    lines = [line.strip() for line in text.split("\n") if line.strip()]  # Remove empty lines

    result = {
        "Delivery Challan": {},
        "Product Details": [],
        "Grand Total": None,
        "Amount in Words": None
    }

    i = 0
    product_table_start = False  # Flag to identify product table start

    while i < len(lines):
        line = lines[i]
        if "Delivery Challan(GOA)" in line:
            result["Delivery Challan"]["Company Name"] = lines[i + 1]
            result["Delivery Challan"]["Address"] = lines[i + 2] + " " + lines[i + 3]
            result["Delivery Challan"]["Email"] = lines[i + 4]
            result["Delivery Challan"]["Associate"] = lines[i + 6]
            result["Delivery Challan"]["Store ID"] = lines[i + 8]

            for j in range(i, min(i + 15, len(lines))):
                match = re.search(r"\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1}", lines[j])
                if match:
                    result["Delivery Challan"]["GSTIN"] = match.group()
                    break
        if "Tax is payable on Reverse Charge" in line:
            result["Delivery Challan"]["Reverse Charge Applicable"] = line.split(":")[-1].strip()

        elif "Type" in line:
            result["Delivery Challan"]["Type"] = lines[i + 1].strip()

        elif "Delivery Challan No." in line:
            result["Delivery Challan"]["Challan No"] = lines[i + 1].strip()

        elif "Place of Supply" in line:
            result["Delivery Challan"]["Place of Supply"] = lines[i + 1].strip()

        elif "Delivery Challan Date" in line:
            result["Delivery Challan"]["Challan Date"] = lines[i + 1].strip()

        elif "Total CP" in line:
            product_table_start = True  # The next valid number should be SNo

        elif product_table_start and re.match(r"^\d+$", line):  # Matches SNo (single number)
            sno = int(lines[i])  # Capture first valid number after "Total CP" as SNo
            item_no = lines[i + 1].strip()  # Next line contains Item#

            description = []
            j = i + 2
            while not re.match(r"^M[A-Za-z0-9]+$", lines[j]):
                description.append(lines[j])
                j += 1
            product_description = " ".join(description)

            # Extract remaining fields
            style = lines[j]
            hsn_code = lines[j + 1]
            tax_percent = int(lines[j + 2])
            quantity = int(lines[j + 3])

            # Extract Invoice# (Sales Order No. + Order Date)
            invoice_line = lines[j + 4].rsplit("/", 1)  # Split at last '/'
            sales_order = invoice_line[0].strip()
            order_date = invoice_line[1].strip()
            year = lines[j + 5].strip()  # Next line contains the year

            # Extract pricing details
            unit_cp = float(lines[j + 6])
            total_cp = float(lines[j + 7])

            # Store product entry
            result["Product Details"].append({
                "SNo": sno,
                "Item#": item_no,
                "Product Description": product_description,
                "Style": style,
                "HSN Code": hsn_code,
                "Tax%": tax_percent,
                "Quantity": quantity,
                "Invoice#": sales_order,
                "Order Date": f"{order_date} {year}",
                "Vendor Unit CP": unit_cp,
                "Total CP": total_cp
            })

            i = j + 7  # Move to the next product entry

        elif "Grand Total" in line:
            for j in range(i, min(i + 3, len(lines))):  # Check next 3 lines for a valid number
                total_match = re.search(r"(\d+[\.,]?\d*)", lines[j])
                if total_match:
                    result["Grand Total"] = float(total_match.group().replace(",", ""))
                    break

        elif "Amount in Words" in line:
            amount_text = line.split(":-")[-1].strip()
            j = i + 1
            while j < len(lines) and not re.match(r"^\d", lines[j]):  # Stop if next line starts with a digit (new section)
                amount_text += " " + lines[j].strip()
                j += 1
            result["Amount in Words"] = amount_text.replace('\u20b9', '').strip()
        i += 1

        return_payload = {
            
        }


    return result

# # Usage
# pdf_path = "rtv4.pdf"
# taj_rtv_data = extract_taj_rtv_details(pdf_path)

# # Print extracted data as JSON
# print(json.dumps(taj_rtv_data, indent=4))

