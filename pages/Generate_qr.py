import streamlit as st
import qrcode
from qrcode.constants import ERROR_CORRECT_H
from urllib.parse import urlencode
from PIL import Image
import requests
import io

def download_logo(url, output_path="logo.png"):
    """Download a PNG logo from a URL and save it locally."""
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        return output_path
    return None

def generate_upi_qr_with_logo(payee_vpa, payee_name, amount=None, transaction_note="Payment", 
                               currency="INR", merchant_code=None, transaction_id=None, 
                               transaction_ref=None, url=None, logo_path=None):
    """Generate a UPI QR code with an optional logo in the center."""
    
    params = {
        "pa": payee_vpa,
        "pn": payee_name,
        "tn": transaction_note,
        "cu": currency,
    }
    
    if amount:
        params["am"] = amount
    if merchant_code:
        params["mc"] = merchant_code
    if transaction_id:
        params["tid"] = transaction_id
    if transaction_ref:
        params["tr"] = transaction_ref
    if url:
        params["url"] = url

    upi_link = f"upi://pay?{urlencode(params)}"

    qr = qrcode.QRCode(version=6, error_correction=ERROR_CORRECT_H, box_size=10, border=4)
    qr.add_data(upi_link)
    qr.make(fit=True)
    
    qr_img = qr.make_image(fill="black", back_color="white").convert("RGBA")

    if logo_path:
        logo = Image.open(logo_path)
        logo_size = qr_img.size[0] // 4  # 1/4th of QR code
        logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
        pos = ((qr_img.size[0] - logo_size) // 2, (qr_img.size[1] - logo_size) // 2)
        qr_img.paste(logo, pos, mask=logo)

    return qr_img

# Streamlit UI
st.title("UPI QR Code Generator")
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Enter Payment Details")
    with st.form("upi_form"):
        payee_vpa="8287604572@yescred"
        payee_name="MINAKI"
        amount = st.text_input("Amount (Optional)", "100.00")
        transaction_note = st.text_input("Transaction Note", "Payment for Order")
        transaction_id = st.text_input("Transaction ID (Optional)")
        transaction_ref = st.text_input("Transaction Reference (Optional)")
        url = st.text_input("Invoice URL (Optional)")
        submitted = st.form_submit_button("Generate QR Code")

with col2:
    if submitted:
        st.subheader("Generated QR Code")
        logo_path = download_logo("https://cdn.shopify.com/s/files/1/0499/5222/7485/files/MINAKI_Logo.png?v=1743416983")
        qr_img = generate_upi_qr_with_logo(payee_vpa, payee_name, amount, transaction_note, 
                                           transaction_id=transaction_id, transaction_ref=transaction_ref, url=url, 
                                           logo_path=logo_path)
        st.image(qr_img, caption="Scan to Pay")
