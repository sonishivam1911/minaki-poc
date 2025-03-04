import math
import pandas as pd
import logging
from collections import defaultdict
from utils.zakya_api import create_item_group, create_sales_order, fetch_sales_order, create_invoice
from utils.postgres_connector import crud

def process_taj_sales(taj_sales_df, invoice_date, access_token, organization_id):
    # Step 1: Load all mapping tables
    mapping_product = crud.read_table("mapping__product")
    mapping_order = crud.read_table("mapping__order")
    mapping_partner = crud.read_table("mapping__partner")

    # Step 2: Prepare lookup sets
    existing_skus = set(mapping_product["SKU"].dropna().astype(str))
    existing_sales_orders = set(mapping_order["salesorder_number"].dropna().astype(str))
    
    sales_orders_payload = defaultdict(list)
    invoices_payload = defaultdict(lambda: {"line_items": []})
    sku_to_item_id = {}
    
    for _, row in taj_sales_df.iterrows():
        sku = row.get("Style", "").strip()
        branch_name = row.get("Branch Name", "").strip()
        party_doc_no = row.get("PartyDoc No", "").strip()
        party_doc_dt = row.get("PartyDoc DT", "").strip()
        quantity = row.get("Qty", 0)
        total = math.ceil(row.get("Total", 0))
        hsn_code = row.get("HSN Code", "")
        tax_name = row.get("Tax Name", "")
        prod_name = row.get("PrintName", "")
        item_dept = row.get("Item Department", "").strip()
        iname = row.get("Item Name", "").strip()

        brand = "MINAKI Menz" if tax_name == "12%" else "MINAKI"
        unit = "pair" if item_dept == "COSTUME JEWELLERY" and "EARRING" in iname else "pcs"

        customer_data = mapping_partner[mapping_partner["display_name"] == branch_name]
        if customer_data.empty:
            logging.error(f"Customer not found for branch: {branch_name}")
            continue
        customer_id = customer_data.iloc[0]["customer_id"]
        
        tax_id = "1923531000000027454" if item_dept == "MENS GARMENT" and customer_data.iloc[0]["Place of Supply"] != "DL" else "1923531000000027518"
        
        if sku not in existing_skus:
            product_data = {
                "group_name": prod_name,
                "brand": brand,
                "unit": unit,
                "status": "active",
                "tax_id": tax_id,
                "item_type": "inventory",
                "is_taxable": True,
                "items": [{
                    "name": prod_name,
                    "rate": total,
                    "purchase_rate": total,
                    "sku": sku
                }]
            }
            response = create_item_group(access_token, organization_id, product_data)
            item_id = response.get("item_id")
            existing_skus.add(sku)
            sku_to_item_id[sku] = item_id
        else:
            item_id = mapping_product.loc[mapping_product["SKU"] == sku, "item_id"].values[0]

        if party_doc_no not in existing_sales_orders:
            sales_orders_payload[party_doc_no].append({
                "item_id": item_id,
                "rate": total,
                "quantity": quantity,
                "hsn_or_sac": hsn_code
            })
    
    for salesorder_number, line_items in sales_orders_payload.items():
        payload = {
            "customer_id": customer_id,
            "salesorder_number": salesorder_number,
            "date": party_doc_dt.strftime("%Y-%m-%d"),
            "line_items": line_items
        }
        create_sales_order(access_token, organization_id, payload)
    
    all_sales_orders = fetch_sales_order(access_token, organization_id)
    salesorder_map = {order["salesorder_number"]: order["salesorder_id"] for order in all_sales_orders}
    item_sales_map = {}
    for order in all_sales_orders:
        for item in order["line_items"]:
            item_sales_map[(order["salesorder_id"], item["item_id"])] = item["salesorder_item_id"]
    
    for _, row in taj_sales_df.iterrows():
        
        sku = row.get("Style", "").strip()
        branch_name = row.get("Branch Name", "").strip()
        party_doc_no = row.get("PartyDoc No", "").strip()
        party_doc_dt = row.get("PartyDoc DT", "").strip()
        quantity = row.get("Qty", 0)
        total = math.ceil(row.get("Total", 0))
        hsn_code = row.get("HSN Code", "")
        tax_name = row.get("Tax Name", "")
        prod_name = row.get("PrintName", "")
        item_dept = row.get("Item Department", "").strip()
        iname = row.get("Item Name", "").strip()

        brand = "MINAKI Menz" if tax_name == "12%" else "MINAKI"
        unit = "pair" if item_dept == "COSTUME JEWELLERY" and "EARRING" in iname else "pcs"

        customer_data = mapping_partner[mapping_partner["display_name"] == branch_name]
        if customer_data.empty:
            logging.error(f"Customer not found for branch: {branch_name}")
            continue
        customer_id = customer_data.iloc[0]["customer_id"]
        gst = customer_data.iloc[0]["gstin"]
        invbr = customer_data.iloc[0]["Customer Number"]
        
        tax_id = "1923531000000027454" if item_dept == "MENS GARMENT" and customer_data.iloc[0]["Place of Supply"] != "DL" else "1923531000000027518"
        salesorder_id = salesorder_map.get(party_doc_no)
        item_id = sku_to_item_id.get(sku)
        salesorder_item_id = item_sales_map.get((salesorder_id, item_id))

        if not salesorder_item_id:
            logging.warning(f"Missing salesorder_item_id for {sku} in {party_doc_no}")
            continue

        invoices_payload[customer_id]["line_items"].append({
            "item_id": item_id,
            "salesorder_item_id": salesorder_item_id,
            "name": prod_name,
            "description": prod_name,
            "rate": total,
            "quantity": quantity
        })
    
    invoice_summary = []
    for customer_id, data in invoices_payload.items():
        invoice_payload = {
            "customer_id": customer_id,
            "invoice_number": invbr,
            "date": invoice_date.strftime("%Y-%m-%d"),
            "payment_terms": 30,
            "exchange_rate": 1.0,
            "line_items": data["line_items"],
            "gst_no": gst,
            "gst_treatment": "business_gst",
            "template_name": "Taj"
        }
        invoice_response = create_invoice(access_token, organization_id, invoice_payload)
        invoice_summary.append({
            "invoice_id": invoice_response.get("invoice_id"),
            "invoice_number": invoice_response.get("invoice_number"),
            "customer_name": branch_name,
            "date": invoice_payload["date"],
            "due_date": invoice_date.strftime("%Y-%m-%d"),
            "amount": sum(item["rate"] * item["quantity"] for item in line_items)
        })
    
    return pd.DataFrame(invoice_summary)
