import pandas as pd
import logic_layer as logic
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import streamlit as st # Dùng st.secrets để đọc config

# Giả lập môi trường log đơn giản
print("🚀 START AUTO RUN...")

try:
    # 1. Kết nối
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    sh = logic.get_master_sheet(creds)
    wks = sh.worksheet(logic.SHEET_CONFIG)
    
    # 2. Đọc Config (Token thật nằm ở đây)
    df = get_as_dataframe(wks, dtype=str).dropna(how='all')
    
    # 3. Chạy vòng lặp
    count_ok = 0
    for idx, row in df.iterrows():
        # Chỉ chạy những dòng Active
        if row.get(logic.COL_STATUS) == "Active":
            print(f"🔄 Đang chạy: {row.get(logic.COL_URL)}...")
            ok, msg, count = logic.sync_data(creds, row)
            
            # Cập nhật kết quả vào Sheet Config
            df.at[idx, logic.COL_RESULT] = msg
            df.at[idx, logic.COL_COUNT] = count
            if ok: count_ok += 1
            print(f"   -> Kết quả: {msg} ({count} dòng)")

    # 4. Lưu lại trạng thái cập nhật
    wks.clear()
    set_with_dataframe(wks, df)
    
    logic.log_system(creds, f"Auto Run hoàn tất. Thành công: {count_ok} job.")
    print("✅ DONE.")

except Exception as e:
    print(f"❌ FATAL ERROR: {e}")
