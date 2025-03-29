import re
import pandas as pd

from config.logger import logger
from utils.postgres_connector import crud
from queries.zakya import queries
from config.settings import XUPING_CATEGORY_MAPPNG



def fetch_category_mapping():
    category_df = crud.execute_query(query=queries.fetch_all_category_mapping,return_data=True)
    #logger.debug(f"Category mapping is : {category_df}")
    category_dict = dict(zip(category_df['category_name'], category_df['category_id']))
    return category_dict,category_df

def create_xuping_sku(category_name):
    try:
        #logger.debug(f"Selected category is: {category_name}")
        prefix = XUPING_CATEGORY_MAPPNG[category_name].lower()
        fetch_next_sku_query = queries.fetch_next_sku.format(prefix=prefix,prefix_length=len(prefix)+1)
        #logger.debug(f"Query is is {fetch_next_sku_query}")
        new_sku = crud.execute_query(fetch_next_sku_query,return_data=True)
        return new_sku["new_sku"][0]
    except Exception as e:
        logger.error(f"Error in create_sku: {e}")
        raise        
