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

salesorder_product_metrics_query = """

WITH product_metrics AS (
    SELECT 
        p.item_id,
        p.item_name,
        p.sku,
        p.category_name,
        som.salesorder_id,
        som.quantity,
        som.rate AS order_rate,
        som.amount,
        so.salesorder_number AS so_number,
        so.date AS order_date,
        so.customer_name,
        so.total AS order_total
    FROM 
        public.zakya_salesorder_line_item_mapping som
    LEFT JOIN 
        public.zakya_products p ON som.item_id = p.item_id
    LEFT JOIN
        public.zakya_sales_order so ON som.salesorder_id = so.salesorder_id
    LEFT JOIN 
        public.zakya_contacts c ON so.customer_id = c.contact_id
    WHERE so.customer_name IS NOT NULL AND c.gst_treatment = 'business_gst'
)

SELECT 
    item_id,
    item_name,
    sku,
    category_name,
    salesorder_id,
    so_number AS salesorder_number,
    TO_DATE(order_date, 'YYYY-MM-DD') AS order_date,
    customer_name,
    SUM(quantity) AS total_quantity,
    SUM(amount) AS total_item_revenue,
    order_total AS total_order_value 
FROM 
    product_metrics
GROUP BY 
    item_id, item_name, sku, category_name,so_number, salesorder_id,
    order_date, customer_name, order_total
ORDER BY 
    order_date DESC, salesorder_number;

"""
