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