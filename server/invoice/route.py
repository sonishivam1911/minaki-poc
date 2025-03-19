from server.invoice.Aza import AzaInvoiceProcessor
from server.invoice.Taj import TajInvoiceProcessor
from server.invoice.Pernia import PerniaInvoiceProcessor

# Wrapper functions for backward compatibility
def process_taj_sales(taj_sales_df, invoice_date, zakya_connection_object):
    """Process Taj sales data using the new class-based approach."""
    processor = TajInvoiceProcessor(taj_sales_df, invoice_date, zakya_connection_object)
    return processor.process()

def process_aza_sales(aza_sales_df, invoice_date, customer_name, zakya_connection_object):
    """Process Aza sales data using the new class-based approach."""
    processor = AzaInvoiceProcessor(aza_sales_df, invoice_date, zakya_connection_object, customer_name)
    return processor.process()

# Add this wrapper function alongside your existing functions
def process_pernia_sales(pernia_sales_df, invoice_date, customer_name, zakya_connection_object):
    """Process Pernia sales data using the new class-based approach."""
    processor = PerniaInvoiceProcessor(pernia_sales_df, invoice_date, zakya_connection_object, customer_name)
    return processor.process()