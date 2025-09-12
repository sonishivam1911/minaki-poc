import requests
from typing import Dict, Any, Optional
from config.logger import logger

class ShopifyGraphQLConnector:
    """
    Shopify GraphQL API connector that extends your existing REST functionality.
    """
    
    def __init__(self, shop_url: str, api_version: str, access_token: str):
        """
        Initialize the GraphQL connector.
        
        Args:
            shop_url: Your shop URL (e.g., 'your-shop.myshopify.com')
            api_version: API version (e.g., '2023-10')
            access_token: Your private app access token
        """
        self.shop_url = shop_url.replace('https://', '').replace('http://', '')
        self.api_version = api_version
        self.access_token = access_token
        self.graphql_endpoint = f"https://{self.shop_url}/admin/api/{api_version}/graphql.json"
        
        # Setup headers for GraphQL requests
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': access_token
        }
    
    def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL query against Shopify's API.
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            Dictionary containing the response data
        """
        try:
            payload = {
                'query': query
            }
            
            if variables:
                payload['variables'] = variables
            
            logger.info(f"Executing GraphQL query to {self.graphql_endpoint}")
            
            response = requests.post(
                self.graphql_endpoint,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            # Check for GraphQL errors
            if 'errors' in result:
                logger.error(f"GraphQL errors: {result['errors']}")
                raise Exception(f"GraphQL errors: {result['errors']}")
            
            logger.info("GraphQL query executed successfully")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"GraphQL query execution failed: {str(e)}")
            raise
    
    def get_products(self, first: int = 10, after: Optional[str] = None, 
                    query_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch products using GraphQL with pagination.
        
        Args:
            first: Number of products to fetch
            after: Cursor for pagination
            query_filter: Optional search query (e.g., 'tag:summer')
            
        Returns:
            Products data with pagination info
        """
        query = """
        query getProducts($first: Int!, $after: String, $query: String) {
            products(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        title
                        handle
                        descriptionHtml
                        productType
                        vendor
                        tags
                        status
                        createdAt
                        updatedAt
                        publishedAt
                        totalInventory
                        featuredImage {
                            id
                            url
                            altText
                        }
                        images(first: 5) {
                            edges {
                                node {
                                    id
                                    url
                                    altText
                                }
                            }
                        }
                        variants(first: 10) {
                            edges {
                                node {
                                    id
                                    title
                                    price
                                    sku
                                    inventoryQuantity
                                    availableForSale
                                }
                            }
                        }
                        metafields(first: 10) {
                            edges {
                                node {
                                    id
                                    namespace
                                    key
                                    value
                                    type
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
            }
        }
        """
        
        variables = {
            'first': first
        }
        
        if after:
            variables['after'] = after
            
        if query_filter:
            variables['query'] = query_filter
        
        return self.execute_query(query, variables)
    
    def get_product_by_id(self, product_id: str) -> Dict[str, Any]:
        """
        Get a single product by ID with full details.
        
        Args:
            product_id: Shopify product ID (with gid://shopify/Product/ prefix)
            
        Returns:
            Product data
        """
        # Ensure proper GraphQL ID format
        if not product_id.startswith('gid://shopify/Product/'):
            product_id = f"gid://shopify/Product/{product_id}"
        
        query = """
        query getProduct($id: ID!) {
            product(id: $id) {
                id
                title
                handle
                descriptionHtml
                productType
                vendor
                tags
                status
                createdAt
                updatedAt
                publishedAt
                totalInventory
                seo {
                    title
                    description
                }
                featuredImage {
                    id
                    url
                    altText
                    width
                    height
                }
                images(first: 20) {
                    edges {
                        node {
                            id
                            url
                            altText
                            width
                            height
                        }
                    }
                }
                variants(first: 100) {
                    edges {
                        node {
                            id
                            title
                            price
                            compareAtPrice
                            sku
                            barcode
                            inventoryQuantity
                            availableForSale
                            weight
                            weightUnit
                            requiresShipping
                            taxable
                        }
                    }
                }
                metafields(first: 50) {
                    edges {
                        node {
                            id
                            namespace
                            key
                            value
                            type
                            description
                        }
                    }
                }
            }
        }
        """
        
        variables = {'id': product_id}
        return self.execute_query(query, variables)
    
    def get_collections(self, first: int = 10, after: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch collections using GraphQL.
        
        Args:
            first: Number of collections to fetch
            after: Cursor for pagination
            
        Returns:
            Collections data with pagination info
        """
        query = """
        query getCollections($first: Int!, $after: String) {
            collections(first: $first, after: $after) {
                edges {
                    node {
                        id
                        title
                        handle
                        description
                        descriptionHtml
                        updatedAt
                        sortOrder
                        templateSuffix
                        productsCount
                        image {
                            id
                            url
                            altText
                        }
                        products(first: 5) {
                            edges {
                                node {
                                    id
                                    title
                                    handle
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
            }
        }
        """
        
        variables = {
            'first': first
        }
        
        if after:
            variables['after'] = after
        
        return self.execute_query(query, variables)
    
    def get_orders(self, first: int = 10, after: Optional[str] = None, 
                  query_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch orders using GraphQL.
        
        Args:
            first: Number of orders to fetch
            after: Cursor for pagination
            query_filter: Optional filter (e.g., 'financial_status:paid')
            
        Returns:
            Orders data with pagination info
        """
        query = """
        query getOrders($first: Int!, $after: String, $query: String) {
            orders(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        name
                        email
                        createdAt
                        updatedAt
                        processedAt
                        financialStatus
                        fulfillmentStatus
                        totalPriceSet {
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        subtotalPriceSet {
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalTaxSet {
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        customer {
                            id
                            firstName
                            lastName
                            email
                        }
                        lineItems(first: 20) {
                            edges {
                                node {
                                    id
                                    title
                                    quantity
                                    originalUnitPriceSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    product {
                                        id
                                        title
                                        handle
                                    }
                                    variant {
                                        id
                                        title
                                        sku
                                    }
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
            }
        }
        """
        
        variables = {
            'first': first
        }
        
        if after:
            variables['after'] = after
            
        if query_filter:
            variables['query'] = query_filter
        
        return self.execute_query(query, variables)
    
    def create_product(self, product_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new product using GraphQL mutation.
        
        Args:
            product_input: Product data dictionary
            
        Returns:
            Created product data
        """
        mutation = """
        mutation productCreate($input: ProductInput!) {
            productCreate(input: $input) {
                product {
                    id
                    title
                    handle
                    status
                    createdAt
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        variables = {
            'input': product_input
        }
        
        return self.execute_query(mutation, variables)
    
    def update_product(self, product_id: str, product_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a product using GraphQL mutation.
        
        Args:
            product_id: Shopify product ID
            product_input: Updated product data
            
        Returns:
            Updated product data
        """
        # Ensure proper GraphQL ID format
        if not product_id.startswith('gid://shopify/Product/'):
            product_id = f"gid://shopify/Product/{product_id}"
        
        mutation = """
        mutation productUpdate($input: ProductInput!) {
            productUpdate(input: $input) {
                product {
                    id
                    title
                    handle
                    status
                    updatedAt
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        # Add the ID to the input
        product_input['id'] = product_id
        
        variables = {
            'input': product_input
        }
        
        return self.execute_query(mutation, variables)
    
    def create_metafield(self, owner_id: str, metafield_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a metafield using GraphQL mutation.
        
        Args:
            owner_id: ID of the resource that owns the metafield
            metafield_input: Metafield data
            
        Returns:
            Created metafield data
        """
        mutation = """
        mutation metafieldSet($metafields: [MetafieldsSetInput!]!) {
            metafieldsSet(metafields: $metafields) {
                metafields {
                    id
                    namespace
                    key
                    value
                    type
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        # Add owner ID to metafield input
        metafield_input['ownerId'] = owner_id
        
        variables = {
            'metafields': [metafield_input]
        }
        
        return self.execute_query(mutation, variables)
    
    def bulk_operation_status(self, operation_id: str) -> Dict[str, Any]:
        """
        Check the status of a bulk operation.
        
        Args:
            operation_id: Bulk operation ID
            
        Returns:
            Operation status data
        """
        query = """
        query getBulkOperation($id: ID!) {
            node(id: $id) {
                ... on BulkOperation {
                    id
                    status
                    errorCode
                    createdAt
                    completedAt
                    objectCount
                    fileSize
                    url
                    partialDataUrl
                }
            }
        }
        """
        
        variables = {'id': operation_id}
        return self.execute_query(query, variables)
    
    def start_bulk_products_export(self, query_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Start a bulk export of products for large datasets.
        
        Args:
            query_filter: Optional filter for products
            
        Returns:
            Bulk operation data
        """
        bulk_query = """
        {
            products {
                edges {
                    node {
                        id
                        title
                        handle
                        productType
                        vendor
                        tags
                        status
                        createdAt
                        updatedAt
                        variants {
                            edges {
                                node {
                                    id
                                    sku
                                    price
                                    inventoryQuantity
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        if query_filter:
            bulk_query = f'{{ products(query: "{query_filter}") {{ edges {{ node {{ id title handle productType vendor tags status createdAt updatedAt variants {{ edges {{ node {{ id sku price inventoryQuantity }} }} }} }} }} }} }}'
        
        mutation = """
        mutation bulkOperationRunQuery($query: String!) {
            bulkOperationRunQuery(query: $query) {
                bulkOperation {
                    id
                    status
                    createdAt
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        variables = {
            'query': bulk_query
        }
        
        return self.execute_query(mutation, variables)

#