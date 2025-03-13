from utils.postgres_connector import crud
from queries.zakya import queries
import re
import pandas as pd

def update_product_categories():
    # fetch all products where categories are none and item name / item description is not empty
    # for each product select the appropriate category and then update the item  
    products_df = crud.execute_query(query=queries.fetch_all_products)
    category_df = crud.execute_query(query=queries.fetch_all_category_mapping)
    category_dict = dict(zip(category_df['category_id'], category_df['category_name']))

    categorize_items(products_df,category_df)


def match_item_to_category(item_name, category_mapping):
    """
    Match an item name to the appropriate category from the mapping.
    
    Parameters:
    - item_name (str): The name of the item to categorize
    - category_mapping (list or DataFrame): List of dictionaries with category_id and category_name
                                           or DataFrame with those columns
    
    Returns:
    - dict: The matching category dict with category_id and category_name, or None if no match
    """
    if not item_name:
        return None
    
    # Convert category_mapping to list of dicts if it's a DataFrame
    if isinstance(category_mapping, pd.DataFrame):
        category_mapping = category_mapping.to_dict('records')
    
    # Convert item name to lowercase for case-insensitive matching
    item_name_lower = item_name.lower()
    
    # 1. Direct matching for exact category names
    for category in category_mapping:
        category_name_lower = category["category_name"].lower()
        # Use word boundary to ensure we match complete words
        pattern = r'\b' + re.escape(category_name_lower) + r'\b'
        if re.search(pattern, item_name_lower):
            return category
    
    # 2. Check for specific category types with variations
    # For bracelets/kadas
    if "bracelet" in item_name_lower or "kada" in item_name_lower:
        if "kada" in item_name_lower:
            kadas_category = next((cat for cat in category_mapping if cat["category_name"] == "Kadas"), None)
            if kadas_category:
                return kadas_category
        
        bracelets_category = next((cat for cat in category_mapping if cat["category_name"] == "Bracelets"), None)
        if bracelets_category:
            return bracelets_category
    
    # For earrings - with specific types
    earring_keywords = ["earring", "ear ring", "jhumka", "jhumki", "earrings", "ear rings"]
    if any(keyword in item_name_lower for keyword in earring_keywords):
        # Check for specific earring types
        if "hoop" in item_name_lower:
            hoop_category = next((cat for cat in category_mapping if cat["category_name"] == "Hoop Earrings"), None)
            if hoop_category:
                return hoop_category
        
        if "jhumka" in item_name_lower or "jhumki" in item_name_lower:
            jhumka_category = next((cat for cat in category_mapping if cat["category_name"] == "Jhumka Earrings"), None)
            if jhumka_category:
                return jhumka_category
        
        if "chaand baali" in item_name_lower or "chaand bali" in item_name_lower:
            chaand_category = next((cat for cat in category_mapping if cat["category_name"] == "Chaand Baali Earrings"), None)
            if chaand_category:
                return chaand_category
        
        if "dangler" in item_name_lower:
            dangler_category = next((cat for cat in category_mapping if cat["category_name"] == "Dangler Earrings"), None)
            if dangler_category:
                return dangler_category
        
        if "stud" in item_name_lower:
            stud_category = next((cat for cat in category_mapping if cat["category_name"] == "Stud Earrings"), None)
            if stud_category:
                return stud_category
        
        # Default to generic earrings
        earrings_category = next((cat for cat in category_mapping if cat["category_name"] == "Earrings"), None)
        if earrings_category:
            return earrings_category
    
    # For necklaces - with specific types
    if "necklace" in item_name_lower or "neck piece" in item_name_lower:
        # Check for specific necklace types
        if "choker" in item_name_lower:
            if "set" in item_name_lower:
                choker_set_category = next((cat for cat in category_mapping if cat["category_name"] == "Choker Necklace Set"), None)
                if choker_set_category:
                    return choker_set_category
            
            choker_category = next((cat for cat in category_mapping if cat["category_name"] == "Choker Necklace"), None)
            if choker_category:
                return choker_category
        
        if "collar" in item_name_lower:
            if "set" in item_name_lower:
                collar_set_category = next((cat for cat in category_mapping if cat["category_name"] == "Collar Necklace Set"), None)
                if collar_set_category:
                    return collar_set_category
        
        if "long" in item_name_lower:
            if "set" in item_name_lower:
                long_set_category = next((cat for cat in category_mapping if cat["category_name"] == "Long Necklace Set"), None)
                if long_set_category:
                    return long_set_category
            
            long_category = next((cat for cat in category_mapping if cat["category_name"] == "Long Necklace"), None)
            if long_category:
                return long_category
        
        # Default to generic necklace
        necklace_category = next((cat for cat in category_mapping if cat["category_name"] == "Necklace"), None)
        if necklace_category:
            return necklace_category
    
    # For rings - excluding earrings
    ring_pattern = r'\bring\b|\brings\b'
    earring_pattern = r'earring|ear ring'
    if re.search(ring_pattern, item_name_lower) and not re.search(earring_pattern, item_name_lower):
        rings_category = next((cat for cat in category_mapping if cat["category_name"] == "Rings"), None)
        if rings_category:
            return rings_category
    
    # For head jewelry
    if "maang teeka" in item_name_lower or "maangtikka" in item_name_lower:
        maang_teeka_category = next((cat for cat in category_mapping if cat["category_name"] == "Maang Teeka"), None)
        if maang_teeka_category:
            return maang_teeka_category
    
    if "matha patti" in item_name_lower:
        matha_patti_category = next((cat for cat in category_mapping if cat["category_name"] == "Matha Patti"), None)
        if matha_patti_category:
            return matha_patti_category
    
    if "passa" in item_name_lower:
        passa_category = next((cat for cat in category_mapping if cat["category_name"] == "Passa"), None)
        if passa_category:
            return passa_category
    
    # For hand jewelry
    if "haath phool" in item_name_lower or "hath phool" in item_name_lower:
        haath_phool_category = next((cat for cat in category_mapping if cat["category_name"] == "Haath Phool"), None)
        if haath_phool_category:
            return haath_phool_category
    
    # For jewelry sets
    if ("jewellery" in item_name_lower or "jewelry" in item_name_lower) and "set" in item_name_lower:
        jewelry_set_category = next((cat for cat in category_mapping if cat["category_name"] == "Jewellery Set"), None)
        if jewelry_set_category:
            return jewelry_set_category
    
    # Default to general Jewellery category
    jewellery_category = next((cat for cat in category_mapping if cat["category_name"] == "Jewellery"), None)
    return jewellery_category or None


def categorize_items(df, category_mapping):
    """
    Categorize each item in the DataFrame using the category mapping.
    
    Parameters:
    - df (DataFrame): DataFrame containing items with at least an 'item_name' column
    - category_mapping (list or DataFrame): List of dictionaries with category_id and category_name
                                           or DataFrame with those columns
    
    Returns:
    - DataFrame: Original DataFrame with added 'category_id' and 'category_name' columns
    """
    # Make a copy to avoid modifying the original DataFrame
    result_df = df.copy()
    
    # Initialize category columns if they don't exist
    if 'category_id' not in result_df.columns:
        result_df['category_id'] = None
    
    if 'category_name' not in result_df.columns:
        result_df['category_name'] = None
    
    # Process each row
    for idx, row in result_df.iterrows():
        match = match_item_to_category(row['item_name'], category_mapping)
        if match:
            result_df.at[idx, 'category_id'] = match['category_id']
            result_df.at[idx, 'category_name'] = match['category_name']
    
    return result_df
