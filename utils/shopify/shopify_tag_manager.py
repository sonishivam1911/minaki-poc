import time
import shopify
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from config.logger import logger

@dataclass
class TagOperationResult:
    """Data class to track tag operation results (both add and remove)."""
    product_id: str
    product_title: str
    tag_operated: str
    operation_type: str  # 'add' or 'remove'
    success: bool
    error_message: str = ""
    updated_at: datetime = None

class ShopifyTagManager:
    """
    Service class for managing product tags in Shopify collections.
    Supports both adding and removing tags.
    """
    
    def __init__(self, rate_limit_delay: float = 0.5):
        """
        Initialize the tag manager.
        
        Args:
            rate_limit_delay: Delay between API calls to respect rate limits
        """
        self.rate_limit_delay = rate_limit_delay
        logger.info("ShopifyTagManager initialized")

    def add_tag_to_products(self, product_ids: List[str], tag_to_add: str) -> List[TagOperationResult]:
        """
        Add a specific tag to multiple products.
        
        Args:
            product_ids: List of Shopify product IDs
            tag_to_add: Tag to add to products
            
        Returns:
            List of TagOperationResult objects
        """
        logger.info(f"Starting tag addition: '{tag_to_add}' to {len(product_ids)} products")
        
        results = []
        
        for i, product_id in enumerate(product_ids):
            logger.info(f"Processing product {i+1}/{len(product_ids)}: {product_id}")
            
            result = self._add_tag_to_single_product(product_id, tag_to_add)
            results.append(result)
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        # Log summary
        successful = len([r for r in results if r.success])
        failed = len(results) - successful
        
        logger.info(f"Tag addition completed: {successful} successful, {failed} failed")
        return results

    def _add_tag_to_single_product(self, product_id: str, tag_to_add: str) -> TagOperationResult:
        """
        Add a tag to a single product.
        
        Args:
            product_id: Shopify product ID
            tag_to_add: Tag to add
            
        Returns:
            TagOperationResult object
        """
        result = TagOperationResult(
            product_id=product_id,
            product_title="Unknown",
            tag_operated=tag_to_add,
            operation_type="add",
            success=False
        )
        
        try:
            # Fetch the product
            product = shopify.Product.find(product_id)
            
            if not product:
                result.error_message = "Product not found"
                logger.error(f"Product {product_id} not found")
                return result
            
            result.product_title = product.title
            
            # Get current tags
            current_tags = [tag.strip() for tag in product.tags.split(',') if tag.strip()]
            
            # Check if tag already exists
            if tag_to_add in current_tags:
                result.success = True  # Consider this successful - tag is already there
                result.error_message = "Tag already exists on product"
                logger.info(f"Tag '{tag_to_add}' already exists on product {product.title}")
                return result
            
            # Add the tag
            updated_tags = current_tags + [tag_to_add]
            
            # Update product tags
            product.tags = ', '.join(updated_tags)
            
            # Save the product
            save_success = product.save()
            
            if save_success:
                result.success = True
                result.updated_at = datetime.now()
                logger.info(f"Successfully added tag '{tag_to_add}' to {product.title}")
            else:
                result.error_message = f"Failed to save product: {getattr(product, 'errors', 'Unknown error')}"
                logger.error(f"Failed to save product {product_id}: {result.error_message}")
            
        except Exception as e:
            result.error_message = f"Exception: {str(e)}"
            logger.error(f"Error adding tag to product {product_id}: {str(e)}")
        
        return result

    def remove_tag_from_products(self, product_ids: List[str], tag_to_remove: str) -> List[TagOperationResult]:
        """
        Remove a specific tag from multiple products.
        
        Args:
            product_ids: List of Shopify product IDs
            tag_to_remove: Tag to remove from products
            
        Returns:
            List of TagOperationResult objects
        """
        logger.info(f"Starting tag removal: '{tag_to_remove}' from {len(product_ids)} products")
        
        results = []
        
        for i, product_id in enumerate(product_ids):
            logger.info(f"Processing product {i+1}/{len(product_ids)}: {product_id}")
            
            result = self._remove_tag_from_single_product(product_id, tag_to_remove)
            results.append(result)
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        # Log summary
        successful = len([r for r in results if r.success])
        failed = len(results) - successful
        
        logger.info(f"Tag removal completed: {successful} successful, {failed} failed")
        return results

    def _remove_tag_from_single_product(self, product_id: str, tag_to_remove: str) -> TagOperationResult:
        """
        Remove a tag from a single product.
        
        Args:
            product_id: Shopify product ID
            tag_to_remove: Tag to remove
            
        Returns:
            TagOperationResult object
        """
        result = TagOperationResult(
            product_id=product_id,
            product_title="Unknown",
            tag_operated=tag_to_remove,
            operation_type="remove",
            success=False
        )
        
        try:
            # Fetch the product
            product = shopify.Product.find(product_id)
            
            if not product:
                result.error_message = "Product not found"
                logger.error(f"Product {product_id} not found")
                return result
            
            result.product_title = product.title
            
            # Get current tags
            current_tags = [tag.strip() for tag in product.tags.split(',') if tag.strip()]
            
            # Check if tag exists
            if tag_to_remove not in current_tags:
                result.success = True  # Consider this successful - tag wasn't there anyway
                result.error_message = "Tag not found on product"
                logger.info(f"Tag '{tag_to_remove}' not found on product {product.title}")
                return result
            
            # Remove the tag
            updated_tags = [tag for tag in current_tags if tag != tag_to_remove]
            
            # Update product tags
            product.tags = ', '.join(updated_tags)
            
            # Save the product
            save_success = product.save()
            
            if save_success:
                result.success = True
                result.updated_at = datetime.now()
                logger.info(f"Successfully removed tag '{tag_to_remove}' from {product.title}")
            else:
                result.error_message = f"Failed to save product: {getattr(product, 'errors', 'Unknown error')}"
                logger.error(f"Failed to save product {product_id}: {result.error_message}")
            
        except Exception as e:
            result.error_message = f"Exception: {str(e)}"
            logger.error(f"Error removing tag from product {product_id}: {str(e)}")
        
        return result

    def preview_tag_addition(self, products_for_tag: List[Dict], tag_to_add: str) -> Dict[str, Any]:
        """
        Create a preview of what will happen when adding a tag.
        
        Args:
            products_for_tag: List of product dictionaries that will get the tag
            tag_to_add: Tag that will be added
            
        Returns:
            Dictionary with preview information
        """
        logger.info(f"Creating preview for adding tag '{tag_to_add}' to {len(products_for_tag)} products")
        
        preview = {
            'tag_to_add': tag_to_add,
            'total_products_affected': len(products_for_tag),
            'products_preview': products_for_tag[:10],  # Show first 10 for preview
            'showing_preview_count': min(10, len(products_for_tag)),
            'additional_products': max(0, len(products_for_tag) - 10),
            'estimated_time_seconds': len(products_for_tag) * self.rate_limit_delay,
            'warnings': []
        }
        
        # Add warnings if needed
        if len(products_for_tag) > 100:
            preview['warnings'].append(f"Large operation: {len(products_for_tag)} products will be updated")
        
        if len(products_for_tag) > 500:
            preview['warnings'].append("This operation may take several minutes to complete")
        
        # Check for potential tag conflicts or issues
        if len(tag_to_add) > 50:
            preview['warnings'].append("Tag is quite long - consider using a shorter tag")
        
        if any(char in tag_to_add for char in ['<', '>', '"', '\'']):
            preview['warnings'].append("Tag contains special characters that may cause issues")
        
        return preview

    def preview_tag_removal(self, products_with_tag: List[Dict], tag_to_remove: str) -> Dict[str, Any]:
        """
        Create a preview of what will happen when removing a tag.
        
        Args:
            products_with_tag: List of product dictionaries that have the tag
            tag_to_remove: Tag that will be removed
            
        Returns:
            Dictionary with preview information
        """
        logger.info(f"Creating preview for removing tag '{tag_to_remove}' from {len(products_with_tag)} products")
        
        preview = {
            'tag_to_remove': tag_to_remove,
            'total_products_affected': len(products_with_tag),
            'products_preview': products_with_tag[:10],  # Show first 10 for preview
            'showing_preview_count': min(10, len(products_with_tag)),
            'additional_products': max(0, len(products_with_tag) - 10),
            'estimated_time_seconds': len(products_with_tag) * self.rate_limit_delay,
            'warnings': []
        }
        
        # Add warnings if needed
        if len(products_with_tag) > 100:
            preview['warnings'].append(f"Large operation: {len(products_with_tag)} products will be updated")
        
        if len(products_with_tag) > 500:
            preview['warnings'].append("This operation may take several minutes to complete")
        
        # Analyze tag impact
        single_tag_products = []
        for product in products_with_tag:
            if len(product.get('tags', [])) == 1:
                single_tag_products.append(product)
        
        if single_tag_products:
            preview['warnings'].append(f"{len(single_tag_products)} products will have no tags after removal")
        
        return preview

    def get_products_with_tag(self, tag_analysis: Dict, tag_name: str) -> List[Dict]:
        """
        Get all products that have a specific tag.
        
        Args:
            tag_analysis: Tag analysis dictionary from CollectionResource
            tag_name: Name of the tag to find products for
            
        Returns:
            List of product dictionaries that have the tag
        """
        return tag_analysis.get('products_by_tag', {}).get(tag_name, [])

    def get_products_without_tag(self, products: List[Dict], tag_name: str) -> List[Dict]:
        """
        Get all products that don't have a specific tag.
        
        Args:
            products: List of product dictionaries
            tag_name: Name of the tag to check for
            
        Returns:
            List of product dictionaries that don't have the tag
        """
        products_without_tag = []
        for product in products:
            product_tags = product.get('tags', [])
            if tag_name not in product_tags:
                products_without_tag.append(product)
        
        return products_without_tag

    def validate_tag_operation_request(self, tag_name: str, products_affected: List[Dict], operation_type: str) -> Tuple[bool, List[str]]:
        """
        Validate a tag operation request (add or remove).
        
        Args:
            tag_name: Tag to be added or removed
            products_affected: Products that will be affected
            operation_type: 'add' or 'remove'
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Basic validation
        if not tag_name or not tag_name.strip():
            errors.append("Tag name cannot be empty")
        
        if not products_affected:
            if operation_type == 'add':
                errors.append("No products available to add tag to")
            else:
                errors.append("No products found with this tag")
        
        # Tag name validation
        if len(tag_name) > 255:
            errors.append("Tag name too long (maximum 255 characters)")
        
        if ',' in tag_name:
            errors.append("Tag name cannot contain commas")
        
        # Check for potentially important tags
        important_tag_patterns = [
            'featured', 'bestseller', 'new-arrival', 'sale', 'discount',
            'limited-edition', 'exclusive', 'trending'
        ]
        
        if operation_type == 'remove':
            for pattern in important_tag_patterns:
                if pattern.lower() in tag_name.lower():
                    errors.append(f"Warning: '{tag_name}' appears to be an important business tag")
                    break
        
        # Check if this would affect too many products
        if len(products_affected) > 1000:
            errors.append(f"Warning: This will affect {len(products_affected)} products. Consider breaking into smaller batches.")
        
        is_valid = len([e for e in errors if not e.startswith("Warning:")]) == 0
        return is_valid, errors

    def create_tag_operation_report(self, results: List[TagOperationResult]) -> Dict[str, Any]:
        """
        Create a comprehensive report of tag operation results.
        
        Args:
            results: List of TagOperationResult objects
            
        Returns:
            Dictionary with report data
        """
        if not results:
            return {"status": "no_data", "message": "No results to report"}
        
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        operation_type = results[0].operation_type if results else "unknown"
        
        report = {
            "summary": {
                "total_processed": len(results),
                "successful_operations": len(successful_results),
                "failed_operations": len(failed_results),
                "success_rate": (len(successful_results) / len(results)) * 100 if results else 0,
                "tag_operated": results[0].tag_operated if results else "Unknown",
                "operation_type": operation_type,
                "processing_time": "Calculated based on rate limiting"
            },
            "successful_products": [
                {
                    "product_id": r.product_id,
                    "product_title": r.product_title,
                    "updated_at": r.updated_at.strftime("%Y-%m-%d %H:%M:%S") if r.updated_at else "N/A"
                }
                for r in successful_results
            ],
            "failed_products": [
                {
                    "product_id": r.product_id,
                    "product_title": r.product_title,
                    "error_message": r.error_message
                }
                for r in failed_results
            ],
            "error_analysis": {}
        }
        
        # Analyze error patterns
        error_counts = {}
        for result in failed_results:
            error_type = result.error_message.split(':')[0] if ':' in result.error_message else result.error_message
            error_counts[error_type] = error_counts.get(error_type, 0) + 1
        
        report["error_analysis"] = error_counts
        
        logger.info(f"Created tag {operation_type} report: {report['summary']['success_rate']:.1f}% success rate")
        return report

    def backup_product_tags(self, products: List[Dict]) -> Dict[str, str]:
        """
        Create a backup of current product tags before operation.
        
        Args:
            products: List of product dictionaries
            
        Returns:
            Dictionary mapping product IDs to their current tags
        """
        logger.info(f"Creating tag backup for {len(products)} products")
        
        backup = {}
        for product in products:
            product_id = product.get('id', product.get('product_id', ''))
            current_tags = product.get('tags_string', '')
            backup[product_id] = current_tags
        
        backup['backup_timestamp'] = datetime.now().isoformat()
        backup['backup_count'] = len(products)
        
        logger.info(f"Tag backup created for {len(products)} products")
        return backup

    def estimate_operation_time(self, product_count: int) -> Dict[str, Any]:
        """
        Estimate how long a tag operation will take.
        
        Args:
            product_count: Number of products to process
            
        Returns:
            Dictionary with time estimates
        """
        base_time_per_product = self.rate_limit_delay + 0.2  # API call + processing time
        total_seconds = product_count * base_time_per_product
        
        estimates = {
            "total_seconds": total_seconds,
            "total_minutes": total_seconds / 60,
            "formatted_time": self._format_duration(total_seconds),
            "rate_limit_delay": self.rate_limit_delay,
            "products_per_minute": 60 / base_time_per_product
        }
        
        return estimates

    def _format_duration(self, seconds: float) -> str:
        """Format duration in a human-readable way."""
        if seconds < 60:
            return f"{seconds:.0f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"