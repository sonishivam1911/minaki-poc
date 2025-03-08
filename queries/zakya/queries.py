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