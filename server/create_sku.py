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

def get_sku_serial(sku_identifier):
    # Read existing serial numbers
    df = crud.read_table('mapping.sku_gen')
    
    # If identifier already exists
    if sku_identifier in df['identifier'].values:
        # Get the current serial_no
        current_serial = df.loc[df['identifier'] == sku_identifier, 'serial_no'].max()
        new_serial = int(current_serial) + 1
        
        # Update the serial_no in the table
        set_clause = f"serial_no = {new_serial}"
        condition = f"identifier = '{sku_identifier}'"
        crud.update_table('mapping.sku_gen', set_clause, condition)
    else:
        # Start serial from 110
        new_serial = 110
        
        # Insert new row
        columns = "identifier, serial_no"
        values = f"'{sku_identifier}', {new_serial}"
        crud.insert_into_table('mapping.sku_gen', columns, values)

    # Format final SKU
    sku = f"{sku_identifier}{str(new_serial).zfill(4)}"
    return sku


def create_jewellery_sku(cat, subcat, collection, auth_code, color_code):
    sku = 'M'
    if cat in ('Jewellery Sets', 'Necklaces'):
        if 'choker' in subcat:
            sku = sku + 'S'
        elif 'collar' in subcat:
            sku = sku + 'M'
        elif 'long' in subcat:
            sku = sku + 'L'
        
        if 'kundan' in collection:
            sku = sku + 'K'
        elif 'temple' in collection:
            sku = sku + 'T'
        elif 'eleganza' in collection:
            sku = sku + 'X'
        elif 'crystal' in collection:
            sku = sku + 'D'
        elif 'ss95' in collection:
            sku = sku + 'SS'
        elif 'precious' in collection:
            sku = sku + 'R'
        
        sku = sku + color_code
        
    else:
        if auth_code == 101 : #Xuping
            sku = sku + 'X'
        elif auth_code == 102 : #Pandahall
            sku = sku + 'P'
        elif auth_code == 103 : #Rangeela Bros
            sku = sku + 'T'
        elif auth_code == 104 : #Prime Kundan Jewellery
            sku = sku + 'CP'
        elif 'eleganza' in collection:
            sku = sku + 'CZ'
        elif 'crystal' in collection:
            sku = sku + 'D'
        elif 'lab' in collection:
            sku = sku + 'LD'
        elif 'ss95' in collection:
            sku = sku + 'SS'

        if 'earring' in cat:
            sku = sku + 'E'
        elif 'bracelet' in cat:
            sku = sku + 'B'
        elif 'maang teeka' in cat:
            sku = sku + 'T'
        elif 'matha patti' in cat:
            sku = sku + 'P'
        elif 'ring' in cat:
            sku = sku + 'R'
        elif 'kada' in cat:
            sku = sku + 'K'
        elif 'passa' in cat:
            sku = sku + 'P'
        elif 'haath phool' in cat:
            sku = sku + 'HP'


    return get_sku_serial(sku)