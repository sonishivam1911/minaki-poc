import requests
import datetime
import json
from typing import Dict, Any, Optional, List
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

# Add these methods to your ShopifyGraphQLConnector class

    def update_review_metafields(self, product_id: str, rating: float, review_count: int, 
                            review_html: Optional[str] = None) -> Dict[str, Any]:
        """
        Update review-related metafields for a product.
        
        Args:
            product_id: Shopify product ID (with or without gid prefix)
            rating: Rating value (e.g., 4.5)
            review_count: Number of reviews
            review_html: Optional HTML content for SPR reviews
            
        Returns:
            Operation result
        """
        try:
            # Ensure proper GraphQL ID format
            if not product_id.startswith('gid://shopify/Product/'):
                product_id = f"gid://shopify/Product/{product_id}"
            
            # Prepare metafields to update
            metafields = []
            
            # Rating metafield (JSON format)
            rating_value = {
                "scale_min": "1.0",
                "scale_max": "5.0", 
                "value": str(rating)
            }
            
            metafields.append({
                "ownerId": product_id,
                "namespace": "reviews",
                "key": "rating",
                "value": json.dumps(rating_value),
                "type": "rating"
            })
            
            # Review count metafield
            metafields.append({
                "ownerId": product_id,
                "namespace": "reviews", 
                "key": "rating_count",
                "value": str(review_count),
                "type": "number_integer"
            })
            
            # Optional SPR HTML content
            if review_html:
                metafields.append({
                    "ownerId": product_id,
                    "namespace": "spr",
                    "key": "reviews", 
                    "value": review_html,
                    "type": "multi_line_text_field"
                })
            
            # Execute the mutation
            result = self.bulk_update_metafields(metafields)
            
            logger.info(f"Updated review metafields for product {product_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error updating review metafields for product {product_id}: {str(e)}")
            raise

    def bulk_update_metafields(self, metafields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Update multiple metafields in a single request.
        
        Args:
            metafields: List of metafield objects to update
            
        Returns:
            Mutation result
        """
        mutation = """
        mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
            metafieldsSet(metafields: $metafields) {
                metafields {
                    id
                    namespace
                    key
                    value
                    type
                    createdAt
                    updatedAt
                }
                userErrors {
                    field
                    message
                    code
                }
            }
        }
        """
        
        variables = {
            'metafields': metafields
        }
        
        return self.execute_query(mutation, variables)

    def delete_metafield_by_key(self, product_id: str, namespace: str, key: str) -> Dict[str, Any]:
        """
        Delete a specific metafield by namespace and key.
        
        Args:
            product_id: Shopify product ID
            namespace: Metafield namespace
            key: Metafield key
            
        Returns:
            Deletion result
        """
        try:
            # First, find the metafield to get its ID
            product_result = self.get_product_metafields(product_id, namespace, key)
            
            if not product_result.get('data', {}).get('product', {}).get('metafields', {}).get('edges'):
                logger.warning(f"Metafield {namespace}.{key} not found for product {product_id}")
                return {"success": False, "message": "Metafield not found"}
            
            metafield_id = product_result['data']['product']['metafields']['edges'][0]['node']['id']
            
            # Delete the metafield
            mutation = """
            mutation metafieldDelete($input: MetafieldDeleteInput!) {
                metafieldDelete(input: $input) {
                    deletedId
                    userErrors {
                        field
                        message
                    }
                }
            }
            """
            
            variables = {
                'input': {
                    'id': metafield_id
                }
            }
            
            result = self.execute_query(mutation, variables)
            logger.info(f"Deleted metafield {namespace}.{key} for product {product_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error deleting metafield {namespace}.{key} for product {product_id}: {str(e)}")
            raise

    def get_product_metafields(self, product_id: str, namespace: Optional[str] = None, 
                            key: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metafields for a product with optional filtering.
        
        Args:
            product_id: Shopify product ID
            namespace: Optional namespace filter
            key: Optional key filter
            
        Returns:
            Product metafields data
        """
        # Ensure proper GraphQL ID format
        if not product_id.startswith('gid://shopify/Product/'):
            product_id = f"gid://shopify/Product/{product_id}"
        
        # Build metafields query with filters
        metafields_args = "first: 50"
        if namespace and key:
            metafields_args = f'first: 50, namespace: "{namespace}", key: "{key}"'
        elif namespace:
            metafields_args = f'first: 50, namespace: "{namespace}"'
        
        query = f"""
        query getProductMetafields($id: ID!) {{
            product(id: $id) {{
                id
                title
                metafields({metafields_args}) {{
                    edges {{
                        node {{
                            id
                            namespace
                            key
                            value
                            type
                            description
                            createdAt
                            updatedAt
                        }}
                    }}
                }}
            }}
        }}
        """
        
        variables = {'id': product_id}
        return self.execute_query(query, variables)

    def create_review_summary_metafield(self, product_id: str, reviews_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a comprehensive review summary metafield.
        
        Args:
            product_id: Shopify product ID
            reviews_data: Dictionary containing review summary data
            
        Returns:
            Creation result
        """
        try:
            # Ensure proper GraphQL ID format
            if not product_id.startswith('gid://shopify/Product/'):
                product_id = f"gid://shopify/Product/{product_id}"
            
            # Create comprehensive review summary
            review_summary = {
                "average_rating": reviews_data.get('average_rating', 0),
                "total_reviews": reviews_data.get('total_reviews', 0),
                "rating_distribution": reviews_data.get('rating_distribution', {}),
                "latest_review_date": reviews_data.get('latest_review_date', ''),
                "featured_reviews": reviews_data.get('featured_reviews', []),
                "last_updated": datetime.now().isoformat()
            }
            
            metafield = {
                "ownerId": product_id,
                "namespace": "reviews",
                "key": "summary",
                "value": json.dumps(review_summary),
                "type": "json"
            }
            
            result = self.bulk_update_metafields([metafield])
            logger.info(f"Created review summary metafield for product {product_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error creating review summary for product {product_id}: {str(e)}")
            raise

    def update_product_seo_metafields(self, product_id: str, title_tag: str, 
                                    description_tag: str) -> Dict[str, Any]:
        """
        Update SEO-related metafields for a product.
        
        Args:
            product_id: Shopify product ID
            title_tag: SEO title tag
            description_tag: SEO description tag
            
        Returns:
            Update result
        """
        try:
            # Ensure proper GraphQL ID format
            if not product_id.startswith('gid://shopify/Product/'):
                product_id = f"gid://shopify/Product/{product_id}"
            
            metafields = [
                {
                    "ownerId": product_id,
                    "namespace": "global",
                    "key": "title_tag",
                    "value": title_tag,
                    "type": "single_line_text_field"
                },
                {
                    "ownerId": product_id,
                    "namespace": "global", 
                    "key": "description_tag",
                    "value": description_tag,
                    "type": "multi_line_text_field"
                }
            ]
            
            result = self.bulk_update_metafields(metafields)
            logger.info(f"Updated SEO metafields for product {product_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error updating SEO metafields for product {product_id}: {str(e)}")
            raise

    # Example usage functions
    def update_product_reviews_example():
        """Example of how to update product review metafields."""
        
        # Initialize connector
        connector = ShopifyGraphQLConnector(
            shop_url="your-shop.myshopify.com",
            api_version="2025-01",
            access_token="your_access_token"
        )
        
        # Update reviews for a product
        product_id = "5714011652253"  # Your product ID
        
        # Example 1: Update basic review data
        result = connector.update_review_metafields(
            product_id=product_id,
            rating=4.8,
            review_count=25,
            review_html="<div>Updated review HTML content...</div>"
        )
        
        # Example 2: Create comprehensive review summary
        reviews_data = {
            "average_rating": 4.8,
            "total_reviews": 25,
            "rating_distribution": {
                "5": 18,
                "4": 5, 
                "3": 2,
                "2": 0,
                "1": 0
            },
            "latest_review_date": "2024-12-15",
            "featured_reviews": [
                {"rating": 5, "text": "Excellent quality!", "author": "Customer A"},
                {"rating": 5, "text": "Beautiful design", "author": "Customer B"}
            ]
        }
        
        summary_result = connector.create_review_summary_metafield(
            product_id=product_id,
            reviews_data=reviews_data
        )
        
        # Example 3: Update SEO metafields
        seo_result = connector.update_product_seo_metafields(
            product_id=product_id,
            title_tag="Amazing Product | Best Quality Online Store",
            description_tag="Discover our amazing product with excellent reviews..."
        )
        
        return result, summary_result, seo_result