import streamlit as st
import io
from main import create_sku, aggregated_df,generate_csv_template, process_csv, map_existing_products

st.title("Create New SKUs and Purchase Orders")

# Step 1: Template Download (from previous code)
st.subheader("Download CSV Template")
template_buffer = generate_csv_template()
st.download_button(
    label="Download CSV Template",
    data=template_buffer,
    file_name="template.csv",
    mime="text/csv"
)

# Add text box for exchange rate of dollar near file upload
st.subheader("Dollar Exchange Rate")
dollar_exchange_rate = st.number_input(
    "Enter the exchange rate for 1 USD:",
    min_value=0.0,
    step=0.01,
    format="%.2f"
)

st.write(f"The entered exchange rate is: {dollar_exchange_rate}")

# File uploader for CSV files
st.subheader("Creating Purchase Order CSV and New SKU CSV")
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        df = process_csv(uploaded_file,dollar_exchange_rate)     

        # Separate rows based on IsProductPresent column
        new_sku_df = df[df["IsProductPresent"] == 'N']

        if not new_sku_df.empty:
            st.subheader("New SKUs Created: Download this CSV and Upload on Zakya")
            st.write("The following SKUs need to be created:")
            

            # Generate the CSV for download
            csv_data = create_sku(new_sku_df)
            st.dataframe(csv_data)
            buffer = io.BytesIO()
            csv_data.to_csv(buffer, index=False)
            buffer.seek(0)

            # Add download button
            st.download_button(
                label="Download New SKUs CSV",
                data=buffer,
                file_name="new_skus.csv",
                mime="text/csv"
            )

        else:
            st.info("No new SKUs to be created.")

        # Map existing products and attach product master
        existing_sku_df = map_existing_products(df)

        # print(f"existing dataframe is {existing_sku_df}")

        # Generate the aggregated DataFrame
        purchase_order_df = aggregated_df(new_sku_df,existing_sku_df)

        st.success("Purchase Order Table - Download Purchase Order & Upload on Zakya to update inventory")
        st.write(purchase_order_df)

        # Add download button
        csv_data = purchase_order_df.to_csv(index=False)
        st.download_button(
            label="Download Aggregated CSV",
            data=csv_data,
            file_name="purchase_order.csv",
            mime="text/csv"
        )        
        
        # Store the DF in session state for later use
        st.session_state["df"] = df

    except Exception as e:
        st.error(f"Error processing the file: {e}")
