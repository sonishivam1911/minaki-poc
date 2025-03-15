import math
import asyncio
import pandas as pd

from dotenv import load_dotenv
from collections import defaultdict
from utils.postgres_connector import crud
from config.logger import logger
from schema.zakya_schemas.schema import ZakyaContacts, ZakyaProducts
from utils.zakya_api import post_record_to_zakya
from queries.zakya import queries
from config.constants import (
    customer_mapping_zakya_contacts,
    products_mapping_zakya_products
)

# Load environment variables from .env file
load_dotenv()

async def create_whereclause_fetch_data(pydantic_model, filter_dict, query):
    """Fetch data using where clause asynchronously."""
    try:
        whereClause = crud.build_where_clause(pydantic_model, filter_dict)
        formatted_query = query.format(whereClause=whereClause)
        data = await asyncio.to_thread(crud.execute_query, query=formatted_query, return_data=True)
        return data.to_dict('records')
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return {"error": f"Error fetching data: {e}"}

async def find_product(style):
    """Find a product by style/SKU."""
    items_data = await create_whereclause_fetch_data(ZakyaProducts, {
        products_mapping_zakya_products['style']: {'op': 'eq', 'value': style}
    }, queries.fetch_prodouct_records)    
    return items_data

async def run_limited_tasks(tasks, limit=10):
    """Run tasks with concurrency limit."""
    semaphore = asyncio.Semaphore(limit)
    
    async def run_with_semaphore(task):
        async with semaphore:
            return await task
    
    return await asyncio.gather(*[run_with_semaphore(task) for task in tasks])

async def preprocess_products(taj_sales_df):
    """Find existing products and create mapping."""
    existing_products = []
    missing_products = []
    existing_sku_item_id_mapping = {}
    
    # Prepare product lookup tasks
    product_tasks = []
    product_styles = []
    
    for _, row in taj_sales_df.iterrows():
        style = row.get("Style", "").strip()
        if not style:
            continue
            
        product_tasks.append(find_product(style))
        product_styles.append(style)
    
    # Run product lookup tasks with concurrency limit
    product_results = await run_limited_tasks(product_tasks, limit=10)
    
    # Process product results
    for style, items_data in zip(product_styles, product_results):
        if items_data and not isinstance(items_data, dict) and "error" not in items_data and len(items_data) > 0:
            existing_sku_item_id_mapping[style] = items_data[0]["item_id"]
            existing_products.append(style)
        else:
            missing_products.append(style)
    
    logger.debug(f"missing_products is {missing_products}")
    logger.debug(f"existing_products is {existing_products}")
    
    return {
        "missing_products": missing_products,
        "existing_products": existing_products,
        "existing_sku_item_id_mapping": existing_sku_item_id_mapping
    }

async def create_invoices(taj_sales_df, zakya_connection_object, invoice_object):
    """Create invoices grouped by branch name."""
    # Group by branch name
    branch_to_customer_map = {}
    branch_to_invoice_payload = defaultdict(lambda: {"line_items": []})
    
    for _, row in taj_sales_df.iterrows():
        try:
            sku = row.get("Style", "").strip()
            branch_name = row.get("Branch Name", "").strip()
            quantity = int(row.get("Qty", 0))
            total = math.ceil(row.get("Total", 0))
            prod_name = row.get("PrintName", "")
            
            # Skip empty rows
            if not branch_name or quantity <= 0:
                continue
                
            # Get customer data if not already cached
            if branch_name not in branch_to_customer_map:
                customer_data = await create_whereclause_fetch_data(
                    ZakyaContacts,
                    {
                        customer_mapping_zakya_contacts['branch_name']: {
                            'op': 'eq', 'value': branch_name
                        }
                    }, 
                    queries.fetch_customer_records
                )
                
                if len(customer_data) == 0 or "error" in customer_data:
                    logger.error(f"Customer not found for branch: {branch_name}")
                    continue
                    
                # Cache customer data
                branch_to_customer_map[branch_name] = {
                    "customer_id": customer_data[0]["contact_id"],
                    "gst": customer_data[0].get("gst_no", ""),
                    "invbr": customer_data[0].get("contact_number", ""),
                    "place_of_contact": customer_data[0].get("place_of_contact", "")
                }
            
            # Prepare line item for invoice
            line_item = {
                "name": prod_name,
                "description": f"{sku} - {prod_name}",
                "rate": total,
                "quantity": quantity
            }
            
            # Check if this SKU exists and add item_id only if it does
            if sku in invoice_object.get('existing_sku_item_id_mapping', {}):
                line_item["item_id"] = invoice_object['existing_sku_item_id_mapping'][sku]
            
            # Add line item to the invoice for this branch
            branch_to_invoice_payload[branch_name]["line_items"].append(line_item)
            
        except Exception as e:
            logger.error(f"Error processing row: {e}")
            continue
    
    # Create invoices (one per branch)
    invoice_summary = []
    
    for branch_name, data in branch_to_invoice_payload.items():
        if not data["line_items"]:
            logger.warning(f"No line items for branch {branch_name}, skipping invoice creation")
            continue
            
        customer_data = branch_to_customer_map[branch_name]
        
        # Create invoice payload
        invoice_payload = {
            "customer_id": customer_data["customer_id"],
            "date": invoice_object['invoice_date'].strftime("%Y-%m-%d"),
            "payment_terms": 30,
            "exchange_rate": 1.0,
            "line_items": data["line_items"],
            "gst_treatment": "business_gst",
            "template_id": 1923531000000916001  # Hardcoded template ID
        }
        
        # Add invoice number if available
        # if customer_data.get("invbr"):
        #     invoice_payload["invoice_number"] = customer_data["invbr"]
            
        # Add GST number if available
        if customer_data.get("gst"):
            invoice_payload["gst_no"] = customer_data["gst"]
        
        try:
            logger.debug(f"Creating invoice for {branch_name} with payload: {invoice_payload.keys()} and {len(invoice_payload["line_items"])}")
            invoice_response = post_record_to_zakya(
                zakya_connection_object['base_url'],
                zakya_connection_object['access_token'],
                zakya_connection_object['organization_id'],
                'invoices',
                invoice_payload
            )
            
            if isinstance(invoice_response, dict) and "invoice" in invoice_response:
                invoice_data = invoice_response["invoice"]
                total_amount = sum(item["rate"] * item["quantity"] for item in data["line_items"])
                
                invoice_summary.append({
                    "invoice_id": invoice_data.get("invoice_id"),
                    "invoice_number": invoice_data.get("invoice_number"),
                    "customer_name": branch_name,
                    "date": invoice_payload["date"],
                    "due_date": invoice_data.get("due_date"),
                    "amount": total_amount,
                    "status": "Success"
                })
                logger.info(f"Successfully created invoice for {branch_name}: {invoice_data.get('invoice_number')}")
            else:
                logger.error(f"Invalid invoice response for {branch_name}: {invoice_response}")
                invoice_summary.append({
                    "customer_name": branch_name,
                    "date": invoice_payload["date"],
                    "status": "Failed",
                    "error": str(invoice_response)
                })
        except Exception as e:
            logger.error(f"Error creating invoice for {branch_name}: {e}")
            invoice_summary.append({
                "customer_name": branch_name,
                "date": invoice_payload["date"],
                "status": "Failed",
                "error": str(e)
            })
    
    return pd.DataFrame(invoice_summary) if invoice_summary else pd.DataFrame()

def process_taj_sales(taj_sales_df, invoice_date, zakya_connection_object):
    """Main processing function for Taj sales."""
    # Preprocess the dataframe
    taj_sales_df["Style"] = taj_sales_df["Style"].astype(str) 
    taj_sales_df['Rounded_Total'] = taj_sales_df['Total'].apply(lambda x: math.ceil(x) if x - int(x) >= 0.5 else math.floor(x))
    
    # Find existing products
    product_config = asyncio.run(preprocess_products(taj_sales_df))
    
    # Prepare invoice object
    invoice_object = {
        'invoice_date': invoice_date,
        'existing_sku_item_id_mapping': product_config['existing_sku_item_id_mapping']
    }
    
    # Create invoices
    invoice_df = asyncio.run(create_invoices(taj_sales_df, zakya_connection_object, invoice_object))
    
    return invoice_df