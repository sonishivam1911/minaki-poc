import streamlit as st
from datetime import datetime
from frontend_components.pernia.utils.state_manager import get_zakya_connection
from server.invoice.route import process_pernia_sales
from frontend_components.invoice.create_salesorder_in_zakya import create_missing_salesorder_component

def invoice_tab():
    """Display the invoice tab content."""
    st.subheader("Generate Invoices")
    
    # Verify all requirements are met
    if not st.session_state.get('all_items_mapped', False):
        st.warning("All items must be mapped to sales orders before generating invoices.")
        return
    
    if st.session_state.get('pernia_orders') is None:
        st.warning("No Pernia orders loaded. Cannot generate invoices.")
        return
    
    # Display summary of orders ready for invoicing
    pernia_orders = st.session_state['pernia_orders']
    sales_orders = st.session_state.get('sales_orders')
    
    st.write(f"Ready to generate invoices for {len(pernia_orders)} Pernia orders.")
    
    # Display sales order summary
    if sales_orders is not None and not sales_orders.empty:
        # Group by sales order to show summary
        order_summary = sales_orders.groupby('Order Number').agg({
            'Total Quantity': 'sum',
            'Total Amount': 'sum'
        }).reset_index()
        
        st.write("Sales Orders Summary:")
        st.dataframe(order_summary, use_container_width=True)
    
    # Invoice date selection
    invoice_date = st.date_input(
        "Invoice Date", 
        value=datetime.now().date(),
        help="Select the date to use for the generated invoices"
    )
    
    # Invoice generation button
    if st.button("Generate Invoices", type="primary"):
        generate_invoices(pernia_orders, invoice_date)

def generate_invoices(df, invoice_date):
    """Generate invoices for Pernia orders."""
    with st.spinner("Generating invoices..."):
        # Get connection details for Zakya
        zakya_connection = get_zakya_connection()
        
        # Process the sales data and generate invoice
        customer_name = st.session_state['selected_customer']
        result_df = process_pernia_sales(
            df, 
            invoice_date,  
            customer_name,
            zakya_connection
        )

        if isinstance(result_df, dict):
            result_df['orders'] = st.session_state['pernia_orders']
            create_missing_salesorder_component({**result_df, **zakya_connection})
            
            # Store missing salesorder info for display
            st.session_state['missing_salesorder_info'] = result_df
            
            # Display missing salesorder component
            st.subheader("Missing Sales Orders")
            st.write("Some products require sales orders before invoicing.")
            st.warning("Please create the missing sales orders first.")
            
            # Button to return to Sales Orders tab
            if st.button("Go to Sales Orders Tab"):
                # This will trigger a rerun and focus on the first tab
                st.session_state['active_tab'] = 0
                st.rerun()
        else:
            # Display results
            st.subheader("Invoice Results")
            st.dataframe(result_df, use_container_width=True)
            
            # Show success or error message
            if "Success" in result_df["status"].values:
                successful_invoices = result_df[result_df["status"] == "Success"]
                st.success(f"Successfully generated {len(successful_invoices)} invoices!")
                
                # Display invoice numbers
                for idx, row in successful_invoices.iterrows():
                    st.write(f"Invoice #{row['invoice_number']} created successfully")
                
                # Download button for invoice results
                csv = result_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Invoice Results as CSV",
                    data=csv,
                    file_name=f"invoice_results_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )
            else:
                st.error("Error generating invoices. Please check the details below.")

            # Store the invoice results
            st.session_state['invoices'] = result_df