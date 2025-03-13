import math
import asyncio
from functools import partial
import pandas as pd
import logging
from dotenv import load_dotenv
from collections import defaultdict
from utils.postgres_connector import crud
from config.logger import logger
from schema.zakya_schemas.schema import ZakyaContacts,ZakyaSalesOrder, ZakyaProducts
from utils.zakya_api import fetch_object_for_each_id, post_record_to_zakya
from queries.zakya import queries
from config.constants import (
    customer_mapping_zakya_contacts
    ,salesorder_mapping_zakya
    ,products_mapping_zakya_products
)

# Load environment variables from .env file
load_dotenv()


async def create_whereclause_fetch_data(pydantic_model, filter_dict, query):
    """
    Fetch the row for the specified branch name as a JSON/dict asynchronously.

    Args:
        pydantic_model: Pydantic model for validation.
        filter_dict: Dictionary with filter conditions.
        query: SQL query template.

    Returns:
        dict: Row data as a dictionary.
    """
    try:
        # Filter customer data for the given branch name
        whereClause = crud.build_where_clause(pydantic_model, filter_dict)
        formatted_query = query.format(whereClause=whereClause)
        
        # Run the synchronous database query in a thread pool to make it non-blocking
        data = await asyncio.to_thread(crud.execute_query, query=formatted_query, return_data=True)
        # logger.debug(f"query is {formatted_query} and data is {data}")
        return data.to_dict('records')
    except Exception as e:
        return {"error": f"Error fetching row: {e}"}


async def find_missing_products(style):
    style_cleaned = style  # Remove the color option from SKU
    
    items_data = await create_whereclause_fetch_data(ZakyaProducts, {
        products_mapping_zakya_products['style']: {'op': 'eq', 'value': style_cleaned}
    }, queries.fetch_prodouct_records)    

    return items_data


async def find_missing_salesorder(salesorder_number):
    
    salesorder_data = await create_whereclause_fetch_data(ZakyaSalesOrder, {
        salesorder_mapping_zakya['salesorder_number']: {'op': 'eq', 'value': salesorder_number}
    }, queries.fetch_salesorderid_record) 

    return salesorder_data


async def run_limited_tasks(tasks, limit=10):
    """
    Run tasks with a limit on concurrent execution.
    
    Args:
        tasks: List of coroutines to run
        limit: Maximum number of tasks to run concurrently
        
    Returns:
        List of results in the same order as the input tasks
    """
    semaphore = asyncio.Semaphore(limit)
    
    async def run_with_semaphore(task):
        async with semaphore:
            return await task
    
    return await asyncio.gather(*[run_with_semaphore(task) for task in tasks])


async def preprocess_taj_sales_report(taj_sales_df):
    existing_products = []
    missing_products = []
    existing_sales_orders = []
    missing_sales_orders = []
    existing_sku_item_id_mapping = {}
    existing_salesorder_number_salesorder_id_mapping = {}
    
    # Prepare tasks for both product and sales order lookups
    product_tasks = []
    product_styles = []
    salesorder_tasks = []
    salesorder_numbers = []
    
    for _, row in taj_sales_df.iterrows():
        style = row.get("Style", "").strip()
        salesorder_number = row.get("PartyDoc No", "").split(" ")[-1]
        logger.debug(f"sku is {style} and sales order number is {salesorder_number}")
        
        product_tasks.append(find_missing_products(style))
        product_styles.append(style)
        
        salesorder_tasks.append(find_missing_salesorder(salesorder_number))
        salesorder_numbers.append(salesorder_number)
    
    # Run product lookup tasks with concurrency limit
    product_results = await run_limited_tasks(product_tasks, limit=10)
    
    # Process product results
    for style, items_data in zip(product_styles, product_results):
        if items_data and not isinstance(items_data, dict) and "error" not in items_data:
            existing_sku_item_id_mapping[style] = items_data[0]["item_id"]
            existing_products.append(style)
        else:
            missing_products.append(style)
    
    # Run sales order lookup tasks with concurrency limit
    salesorder_results = await run_limited_tasks(salesorder_tasks, limit=10)
    
    # Process sales order results
    for salesorder_number, salesorder_data in zip(salesorder_numbers, salesorder_results):
        if salesorder_data and not isinstance(salesorder_data, dict) and "error" not in salesorder_data:
            existing_salesorder_number_salesorder_id_mapping[salesorder_number] = salesorder_data[0]["salesorder_id"]
            existing_sales_orders.append(salesorder_data)
        else:
            missing_sales_orders.append(salesorder_number)
    
    logger.debug(f"missing_products is {missing_products}")
    logger.debug(f"existing_products is {existing_products}")
    logger.debug(f"existing_sales_orders is {existing_sales_orders}")
    logger.debug(f"missing_sales_orders is {missing_sales_orders}")

    return {
        "missing_products": missing_products,
        "existing_products": existing_products,
        "missing_sales_orders": missing_sales_orders,
        "existing_sales_orders": existing_sales_orders,
        "existing_sku_item_id_mapping": existing_sku_item_id_mapping,
        'existing_salesorder_number_salesorder_id_mapping': existing_salesorder_number_salesorder_id_mapping
    }


async def create_products_and_sales_order(taj_sales_df, zakya_connection_object, config):

    sales_orders_payload = defaultdict(list)
    sales_order_customer_id_mapping = {}
    for _,row in taj_sales_df.iterrows():
        sku = row.get("Style", "").strip()
        branch_name = row.get("Branch Name", "").strip()
        party_doc_no = row.get("PartyDoc No", "").strip().split(" ")[-1]
        party_doc_dt = row.get("PartyDoc DT", "")
        quantity = row.get("Qty", 0)
        total = math.ceil(row.get("Total", 0))
        hsn_code = str(row.get("HSN Code", "")).split(".")[0]
        tax_name = row.get("Tax Name", "")
        prod_name = row.get("PrintName", "")
        item_dept = row.get("Item Department", "").strip()
        iname = row.get("Item Name", "").strip()

        brand = "MINAKI Menz" if tax_name == "12%" else "MINAKI"
        unit = "pair" if item_dept == "COSTUME JEWELLERY" and "EARRING" in iname else "pcs"

        customer_data = await create_whereclause_fetch_data(ZakyaContacts,{
            customer_mapping_zakya_contacts['branch_name'] : {
                'op' : 'eq' , 'value' : branch_name
            }
            }, queries.fetch_customer_records
        )        
        
        tax_id = "1923531000000027454" if item_dept == "MENS GARMENT" and customer_data[0]["place_of_contact"] != "DL" else "1923531000000027518"
        
        if sku in config['missing_products']:
            product_data = {
                "group_name": prod_name,
                "brand": brand,
                "unit": unit,
                "status": "active",
                "tax_id": int(tax_id),
                "item_type": "inventory",
                "is_taxable": True,
                "items": [{
                    "name": prod_name,
                    "rate": total,
                    "purchase_rate": total,
                    "sku": sku
                }]
            }
            logger.debug(f"payload for product data is {product_data} and sku is {sku}")
            response = post_record_to_zakya(
                            zakya_connection_object['base_url'],
                            zakya_connection_object['access_token'],  
                            zakya_connection_object['organization_id'],
                            '/itemgroups',
                            product_data
                            )
            config["existing_sku_item_id_mapping"][sku] = response["item_group"]["items"][0]["item_id"]
            # logger.debug(f"response for creating product is {response["item_group"]["items"][0]["item_id"]}")
            
        if party_doc_no in config["missing_sales_orders"]:
            
            sales_order_customer_id_mapping[party_doc_no] = customer_data[0]["contact_id"]
            sales_orders_payload[party_doc_no].append({
                "item_id": int(config["existing_sku_item_id_mapping"][sku]),
                "rate": int(total),
                "quantity": int(quantity),
                "hsn_or_sac": int(hsn_code)
            })
            logger.debug(f"Sales Order number is : {party_doc_no} and sales order payload is {sales_orders_payload}")
    
    for salesorder_number, line_items in sales_orders_payload.items():
        
        salesorder_payload = {
            "customer_id": int(sales_order_customer_id_mapping[salesorder_number]),
            "salesorder_number": salesorder_number,
            "date": str(party_doc_dt.strftime("%Y-%m-%d")),
            "line_items": line_items,
        }
        logger.debug(f"Sales Order Payload is {salesorder_payload}")
        response = post_record_to_zakya(
                            zakya_connection_object['base_url'],
                            zakya_connection_object['access_token'],  
                            zakya_connection_object['organization_id'],
                            '/salesorders',
                            salesorder_payload
                            )
        config["existing_salesorder_number_salesorder_id_mapping"][salesorder_number] = response["salesorder"]["salesorder_id"]
        # logger.debug(f"Response from creating sales order is : {response["salesorder"]["salesorder_id"]}")


    return config["existing_sku_item_id_mapping"]


def fetch_sales_order(config,zakya_connection_object):

    salesorder_map = {}
    item_sales_map = {}
    for salesorder_id in config["existing_salesorder_number_salesorder_id_mapping"].keys():
        
        sales_order = fetch_object_for_each_id(
            zakya_connection_object["base_url"],
            zakya_connection_object["access_token"],
            zakya_connection_object["organization_id"],
            f'/salesorders/{config["existing_salesorder_number_salesorder_id_mapping"][salesorder_id]}' 
            )

        # logger.debug(f"Sales order returned is {sales_order}")
        salesorder_map[sales_order["salesorder"]["salesorder_number"]] = sales_order["salesorder"]["salesorder_id"] 
        # logger.debug(f"Sales order mapping is {salesorder_map}")

        for item in sales_order["salesorder"]["line_items"]:
            item_sales_map[(sales_order["salesorder"]["salesorder_id"] , item["item_id"])] = item["line_item_id"]
            
    return salesorder_map, item_sales_map


async def create_invoices(taj_sales_df,zakya_connection_object,invoice_object):

    invoices_payload = defaultdict(lambda: {"line_items": []})
    for _, row in taj_sales_df.iterrows():
        
        sku = row.get("Style", "").strip()
        branch_name = row.get("Branch Name", "").strip()
        party_doc_no = row.get("PartyDoc No", "").strip().split(" ")[-1]
        party_doc_dt = row.get("PartyDoc DT", "")
        quantity = row.get("Qty", 0)
        total = math.ceil(row.get("Total", 0))
        hsn_code = row.get("HSN Code", "")
        tax_name = row.get("Tax Name", "")
        prod_name = row.get("PrintName", "")
        item_dept = row.get("Item Department", "").strip()
        iname = row.get("Item Name", "").strip()

        brand = "MINAKI Menz" if tax_name == "12%" else "MINAKI"
        unit = "pair" if item_dept == "COSTUME JEWELLERY" and "EARRING" in iname else "pcs"

        customer_data = await create_whereclause_fetch_data(ZakyaContacts,{
            customer_mapping_zakya_contacts['branch_name'] : {
                'op' : 'eq' , 'value' : branch_name
            }
            }, queries.fetch_customer_records
        ) 

        if len(customer_data) == 0:
            logging.error(f"Customer not found for branch: {branch_name}")
            continue

        customer_id = customer_data[0]["contact_id"]
        gst = customer_data[0]["gst_no"]
        invbr = customer_data[0]["contact_number"]
        
        tax_id = "1923531000000027454" if item_dept == "MENS GARMENT" and customer_data[0]["place_of_contact"] != "DL" else "1923531000000027518"
        salesorder_id = invoice_object["salesorder_map"].get(party_doc_no)
        item_id = invoice_object['sku_to_item_id'].get(sku)
        logger.debug(f"salesorder_id is {salesorder_id} and item_id is {item_id}")
        salesorder_item_id = invoice_object['item_sales_map'].get((salesorder_id, item_id))

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
            "date": invoice_object['invoice_date'].strftime("%Y-%m-%d"),
            "payment_terms": 30,
            "exchange_rate": 1.0,
            "line_items": data["line_items"],
            "gst_no": gst,
            "gst_treatment": "business_gst",
            "template_name": "Taj"
        }
        logger.debug(f"Invoice payload is {invoice_payload}")
        invoice_response = post_record_to_zakya(
                zakya_connection_object['base_url'],
                zakya_connection_object['access_token'],
                zakya_connection_object['organization_id'],
                '/invoices',
                invoice_payload

            )
        logger.debug(f"Invoice Response is  : {invoice_response}")
        invoice_summary.append({
            "invoice_id": invoice_response.get("invoice_id"),
            "invoice_number": invoice_response.get("invoice_number"),
            "customer_name": branch_name,
            "date": invoice_payload["date"],
            "due_date": ['invoice_date'].strftime("%Y-%m-%d"),
            "amount": sum(item["rate"] * item["quantity"] for item in data["line_items"])
        })
    
    return pd.DataFrame(invoice_summary)    

def process_taj_sales(taj_sales_df,invoice_date,zakya_connection_object):

    taj_sales_df["Style"]=taj_sales_df["Style"].astype(str) 
    taj_sales_df['Rounded_Total'] = taj_sales_df['Total'].apply(lambda x: math.ceil(x) if x - int(x) >= 0.5 else math.floor(x))    
    config = asyncio.run(preprocess_taj_sales_report(taj_sales_df))
    sku_to_item_id = asyncio.run(create_products_and_sales_order(taj_sales_df,zakya_connection_object,config))
    salesorder_map, item_sales_map = fetch_sales_order(config,zakya_connection_object)
    invoice_object = {
        'salesorder_map' : salesorder_map,
        'item_sales_map' : item_sales_map,
        'invoice_date' : invoice_date,
        'sku_to_item_id' : sku_to_item_id
    }
    logger.debug(f"salesorder_map is {salesorder_map}")
    logger.debug(f"item_sales_map is {item_sales_map}")
    invoice_df = asyncio.run(create_invoices(taj_sales_df,zakya_connection_object,invoice_object))
    return invoice_df