import math
import asyncio
import logging
from collections import defaultdict
from dotenv import load_dotenv
from utils.postgres_connector import crud
from schema.zakya_schemas.schema import ZakyaContacts, ZakyaSalesOrder, ZakyaProducts
from utils.zakya_api import fetch_object_for_each_id, post_record_to_zakya
from queries.zakya import queries
from config.constants import (
    customer_mapping_zakya_contacts,
    salesorder_mapping_zakya,
    products_mapping_zakya_products,
)

# Load environment variables from .env file
load_dotenv()


async def create_whereclause_fetch_data(pydantic_model, filter_dict, query):
    """
    Fetch data asynchronously from the database based on filter criteria.
    """
    try:
        whereClause = crud.build_where_clause(pydantic_model, filter_dict)
        formatted_query = query.format(whereClause=whereClause)
        data = await asyncio.to_thread(crud.execute_query, query=formatted_query, return_data=True)
        return data.to_dict("records")
    except Exception as e:
        return {"error": f"Error fetching row: {e}"}


async def fetch_existing_data(taj_sales_df):
    """
    Fetch existing products and sales orders from the database.
    Assumes that all sales orders and products exist.
    """
    existing_sku_item_id_mapping = {}
    existing_salesorder_number_salesorder_id_mapping = {}

    # Fetch all products in batch
    product_tasks = [
        create_whereclause_fetch_data(
            ZakyaProducts,
            {products_mapping_zakya_products["style"]: {"op": "eq", "value": row.get("Style", "").strip()}},
            queries.fetch_prodouct_records,
        )
        for _, row in taj_sales_df.iterrows()
    ]
    product_results = await asyncio.gather(*product_tasks)

    for row, items_data in zip(taj_sales_df.itertuples(), product_results):
        style = row.Style.strip()
        if items_data and isinstance(items_data, list) and items_data:
            existing_sku_item_id_mapping[style] = items_data[0]["item_id"]

    # Fetch all sales orders in batch
    salesorder_tasks = [
        create_whereclause_fetch_data(
            ZakyaSalesOrder,
            {salesorder_mapping_zakya["salesorder_number"]: {"op": "eq", "value": row.PartyDocNo.split(" ")[-1]}},
            queries.fetch_salesorderid_record,
        )
        for _, row in taj_sales_df.iterrows()
    ]
    salesorder_results = await asyncio.gather(*salesorder_tasks)

    for row, salesorder_data in zip(taj_sales_df.itertuples(), salesorder_results):
        salesorder_number = row.PartyDocNo.split(" ")[-1]
        if salesorder_data and isinstance(salesorder_data, list) and salesorder_data:
            existing_salesorder_number_salesorder_id_mapping[salesorder_number] = salesorder_data[0]["salesorder_id"]

    return {
        "existing_sku_item_id_mapping": existing_sku_item_id_mapping,
        "existing_salesorder_number_salesorder_id_mapping": existing_salesorder_number_salesorder_id_mapping,
    }


async def create_invoices(taj_sales_df, zakya_connection_object, invoice_object):
    """
    Creates invoices using existing sales orders and products. 
    If a product is missing, it adds the required description, price, and HSN in the invoice payload.
    """
    invoices_payload = defaultdict(lambda: {"line_items": []})

    for _, row in taj_sales_df.iterrows():
        sku = row.get("Style", "").strip()
        branch_name = row.get("Branch Name", "").strip()
        party_doc_no = row.get("PartyDoc No", "").strip().split(" ")[-1]
        party_doc_dt = row.get("PartyDoc DT", "")
        quantity = row.get("Qty", 0)
        total = math.ceil(row.get("Total", 0))
        hsn_code = row.get("HSN Code", "")
        prod_name = row.get("PrintName", "")

        # Fetch customer details
        customer_data = await create_whereclause_fetch_data(
            ZakyaContacts,
            {customer_mapping_zakya_contacts["branch_name"]: {"op": "eq", "value": branch_name}},
            queries.fetch_customer_records,
        )
        if not customer_data:
            logging.error(f"Customer not found for branch: {branch_name}")
            continue

        customer_id = customer_data[0]["contact_id"]
        gst = customer_data[0]["gst_no"]
        invbr = customer_data[0]["contact_number"]

        # Retrieve existing sales order ID
        salesorder_id = invoice_object["salesorder_map"].get(party_doc_no)
        item_id = invoice_object["sku_to_item_id"].get(sku)

        # If the item doesn't exist, provide additional details
        line_item = {
            "item_id": item_id if item_id else None,  # Use existing item ID or None
            "name": prod_name,
            "description": prod_name,
            "rate": total,
            "quantity": quantity,
            "hsn_or_sac": hsn_code,
        }

        if not item_id:
            logging.warning(f"Missing product: {sku}, adding details in line items")

        invoices_payload[customer_id]["line_items"].append(line_item)

    # Send invoice requests
    invoice_summary = []
    for customer_id, data in invoices_payload.items():
        invoice_payload = {
            "customer_id": customer_id,
            "invoice_number": invbr,
            "date": invoice_object["invoice_date"].strftime("%Y-%m-%d"),
            "payment_terms": 30,
            "exchange_rate": 1.0,
            "line_items": data["line_items"],
            "gst_no": gst,
            "gst_treatment": "business_gst",
            "template_id": 1923531000000916001,
        }

        logging.debug(f"Invoice payload: {invoice_payload}")
        invoice_response = post_record_to_zakya(
            zakya_connection_object["base_url"],
            zakya_connection_object["access_token"],
            zakya_connection_object["organization_id"],
            "invoices",
            invoice_payload,
        )
        invoice_summary.append(invoice_response)

    return invoice_summary
