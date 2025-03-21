from utils.postgres_connector import crud
from config.logger import logger
from queries.zakya import queries



def fetch_product_metrics_for_invoice_by_customer():
    try:
        invoicing_analytics = crud.execute_query(queries.invoice_product_mapping_query,True)
        logger.debug(f"Data pulled is :{invoicing_analytics}")
        return invoicing_analytics
    except Exception as e:
        logger.debug(f"Product analytics query failed with error: {e}")

