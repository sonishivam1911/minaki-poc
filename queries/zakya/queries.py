fetch_customer_records = """
select *
from zakya_contacts
{whereClause}
"""

fetch_salesorderid_record = """
select salesorder_id
from zakya_sales_order
{whereClause}
"""

fetch_prodouct_records = """
select *
from zakya_products
{whereClause}
"""


fetch_all_products = """
select *
from zakya_products  
"""

fetch_all_category_mapping = """
select distinct category_id, category_name
from zakya_products
where category_id != ''
"""


fetch_next_sku = """
WITH max_serial AS (
    SELECT 
        MAX(CAST(SUBSTRING(sku, 4) AS INTEGER)) AS max_serial_number
    FROM 
        zakya_products
    WHERE 
        LOWER(sku) LIKE '{prefix}%' 
        AND SUBSTRING(sku, {prefix_length}) ~ '^[0-9]+$'
)
SELECT 
    Upper('{prefix}') || (max_serial_number + 1)::TEXT AS new_sku
FROM 
    max_serial;
"""

# Define the CREATE TABLE SQL statement
create_shiprocket_salesorder_mapping_table_query = """
    CREATE TABLE IF NOT EXISTS shipments (
        id SERIAL PRIMARY KEY,
        sales_order_id BIGINT,
        sales_order_number VARCHAR(50),
        customer_id BIGINT,
        customer_name VARCHAR(255),
        order_date DATE,
        status VARCHAR(50),
        total DECIMAL(12, 2),
        shipping_address TEXT,
        shipping_city VARCHAR(100),
        shipping_state VARCHAR(100),
        shipping_zip VARCHAR(20),
        shipping_country VARCHAR(100),
        shipment_id BIGINT,
        order_id BIGINT,
        awb_code VARCHAR(100),
        courier_name VARCHAR(100),
        pickup_scheduled_date TIMESTAMP,
        pickup_token_number VARCHAR(100),
        routing_code VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create index on sales_order_id for faster lookups
    CREATE INDEX IF NOT EXISTS idx_shipments_sales_order_id ON shipments(sales_order_id);
    
    -- Create index on shipment_id
    CREATE INDEX IF NOT EXISTS idx_shipments_shipment_id ON shipments(shipment_id);
    """

