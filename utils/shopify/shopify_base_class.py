from abc import ABC, abstractmethod
import pandas as pd

from .shopify_connector import ShopifyConnector

class BaseShopifyResource(ABC):
    """
    Abstract base class that lays out the blueprint
    for interacting with Shopify resources.
    """
    
    def __init__(self, connector: ShopifyConnector):
        """
        Each resource class needs a reference to the connector
        so it can perform API calls.
        """
        self.connector = connector

    @abstractmethod
    def get_all(self):
        """
        Retrieve all objects of this resource type
        from Shopify (potentially in a paginated manner).
        """
        pass

    @abstractmethod
    def create(self, **kwargs):
        """
        Create a new object of this resource type in Shopify.
        """
        pass

    def to_dataframe(self, data_list):
        """
        Optional helper to convert retrieved data to a pandas DataFrame.
        Can be overridden if necessary for specialized transformations.
        """
        # By default, just turn list of dicts into DataFrame:
        return pd.DataFrame(data_list)