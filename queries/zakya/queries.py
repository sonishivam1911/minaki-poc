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


invoice_product_mapping_query = """


WITH customer_product_metrics AS (
    -- Join line items with products, invoices, and customers to get complete sales data
    SELECT 
        p.item_id,
        p.name AS product_name,
        p.category_name,
        p.cf_collection AS collection,
        i.customer_id,
        c.contact_name AS customer_name,
        c.company_name,
        c.customer_sub_type as customer_type,
        i.date as invoice_date,
        SUM(lim.quantity) AS total_quantity_sold,
        SUM(lim.amount) AS total_revenue,
        CASE 
            WHEN SUM(lim.quantity) > 0 
            THEN SUM(lim.amount) / SUM(lim.quantity) 
            ELSE 0 
        END AS avg_selling_price
    FROM 
        public.zakya_invoice_line_item_mapping lim
    LEFT JOIN 
        public.zakya_products p ON lim.item_id = p.item_id
    LEFT JOIN 
        public.zakya_invoices i ON lim.invoice_id = i.invoice_id
    LEFT JOIN
        public.zakya_contacts c ON i.customer_id = c.contact_id
    WHERE 
        -- Filter out void/cancelled/draft invoices if needed
        (i.status = 'paid' OR i.status = 'sent' OR i.status = 'overdue')
        AND i.customer_id IS NOT NULL
    GROUP BY 
        p.item_id, p.name, p.category_name, p.cf_collection, 
        i.customer_id, c.contact_name, c.company_name,c.customer_sub_type,invoice_date
)

-- 1. Customer-Product metrics
SELECT 
    customer_id,
    customer_name,
    company_name,
    item_id,
    product_name,
    category_name,
    collection,
    customer_type,
    invoice_date,
    total_quantity_sold,
    total_revenue,
    avg_selling_price
FROM 
    customer_product_metrics
ORDER BY 
    customer_name, total_revenue DESC;

"""