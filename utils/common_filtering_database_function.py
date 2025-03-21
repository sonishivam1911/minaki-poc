from utils.postgres_connector import crud
from config.logger import logger
from config.constants import products_mapping_zakya_products
from schema.zakya_schemas.schema import ZakyaProducts
from queries.zakya import queries

def create_whereclause_fetch_data(pydantic_model, filter_dict, query):
    """Fetch data using where clause asynchronously."""
    try:
        whereClause = crud.build_where_clause(pydantic_model, filter_dict)
        formatted_query = query.format(whereClause=whereClause)
        data = crud.execute_query(query=formatted_query, return_data=True)
        return data.to_dict('records')
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return {"error": f"Error fetching data: {e}"}

def find_product(sku):
    """Find a product by SKU."""
    items_data = create_whereclause_fetch_data(ZakyaProducts, {
        products_mapping_zakya_products['style']: {'op': 'eq', 'value': sku}
    }, queries.fetch_prodouct_records)    
    return items_data
