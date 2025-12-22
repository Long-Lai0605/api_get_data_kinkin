import streamlit as st
import backend as be  # Đổi utils thành backend
import pandas as pd
import time

# --- Cấu hình Secrets cho Backend ---
# Vì backend.py độc lập, ta cần truyền st.secrets vào các hàm của nó
secrets = st.secrets

st.set_page_config(page_title="KINKIN ENGINE", layout="wide")
st.title("🛡️ CONTROL PANEL")

# Load data
blocks = be.get_active_blocks(secrets)

if st.button("CHẠY NGAY"):
    for block in blocks:
        st.write(f"Running {block['Block Name']}...")
        data, msg = be.fetch_1office_data(block['API URL'], block['Access Token (Encrypted)'], block['Method'])
        if data:
            count, _ = be.write_to_sheet(secrets, block, data)
            st.success(f"Success: {count} rows")
        else:
            st.error(f"Failed: {msg}")
