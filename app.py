import streamlit as st
import pandas as pd
import logic_layer as logic
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time

st.set_page_config(page_title="1Office Auto Tool", layout="wide")

def get_creds():
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=['https://www.googleapis.com/auth/spreadsheets']
    )

def load_config(creds):
    sh = logic.get_master_sheet(creds)
    try: wks = sh.worksheet(logic.SHEET_CONFIG)
    except:
        wks = sh.add_worksheet(logic.SHEET_CONFIG, 100, 20)
        # Tạo header chuẩn
        headers = [logic.COL_BLOCK, logic.COL_STATUS, logic.COL_METHOD, logic.COL_URL, logic.COL_KEY, logic.COL_TARGET, logic.COL_RESULT, logic.COL_COUNT]
        wks.append_row(headers)
    
    df = get_as_dataframe(wks, dtype=str).dropna(how='all')
    # Bù cột thiếu
    required = [logic.COL_BLOCK, logic.COL_STATUS, logic.COL_METHOD, logic.COL_URL, logic.COL_KEY, logic.COL_TARGET, logic.COL_RESULT, logic.COL_COUNT]
    for c in required: 
        if c not in df.columns: df[c] = ""
    return df, wks

# --- LOGIC MASKING (CHE TOKEN) ---
def mask_val(val):
    if not val or pd.isna(val) or len(str(val)) < 5: return ""
    if "SECURE" in str(val): return val
    return "••••••_SECURE_STORED"

# --- MAIN UI ---
st.title("🚀 1Office Manager (Tab: luu_api_key)")
creds = get_creds()

try:
    df_config, wks_config = load_config(creds)
except Exception as e:
    st.error("Chưa cấu hình secrets hoặc sai ID Sheet.")
    st.stop()

if logic.COL_BLOCK not in df_config.columns: df_config[logic.COL_BLOCK] = "Default"
all_blocks = df_config[logic.COL_BLOCK].unique().tolist()

# SIDEBAR
block = st.sidebar.selectbox("Chọn Khối:", all_blocks)
if st.sidebar.button("➕ Thêm Khối"):
    new_name = st.sidebar.text_input("Tên:")
    if new_name and new_name not in all_blocks:
        new_row = pd.DataFrame([{logic.COL_BLOCK: new_name, logic.COL_STATUS: "Active"}])
        df_config = pd.concat([df_config, new_row], ignore_index=True)
        wks_config.clear(); set_with_dataframe(wks_config, df_config)
        st.rerun()

# DASHBOARD
st.subheader(f"Cấu hình: {block}")
# Lọc data và Che Token
display_df = df_config[df_config[logic.COL_BLOCK] == block].copy()
display_df[logic.COL_KEY] = display_df[logic.COL_KEY].apply(mask_val)

edited = st.data_editor(
    display_df,
    num_rows="dynamic",
    column_config={
        logic.COL_STATUS: st.column_config.SelectboxColumn(options=["Active", "Stop"]),
        logic.COL_METHOD: st.column_config.SelectboxColumn(options=["GET", "POST"]),
        logic.COL_KEY: st.column_config.TextColumn("API Key (Hidden)", width="large"),
        logic.COL_TARGET: st.column_config.TextColumn("Tab Đích (VD: Data_NS)"),
        logic.COL_RESULT: st.column_config.TextColumn(disabled=True),
        logic.COL_COUNT: st.column_config.NumberColumn(disabled=True),
    },
    use_container_width=True, hide_index=True
)

if st.button("💾 LƯU CẤU HÌNH"):
    # Lấy phần data không thuộc block này giữ nguyên
    df_final = df_config[df_config[logic.COL_BLOCK] != block].copy()
    
    # Lấy data mới sửa
    df_new = edited.copy()
    df_new[logic.COL_BLOCK] = block
    
    # KHÔI PHỤC TOKEN GỐC NẾU USER KHÔNG SỬA
    for idx, row in df_new.iterrows():
        if "SECURE" in str(row[logic.COL_KEY]):
            # Tìm token gốc trong database
            orig = df_config[(df_config[logic.COL_BLOCK] == block)].iloc[idx][logic.COL_KEY]
            df_new.at[idx, logic.COL_KEY] = orig
            
    df_final = pd.concat([df_final, df_new], ignore_index=True)
    wks_config.clear(); set_with_dataframe(wks_config, df_final)
    st.success("Đã lưu vào luu_api_key!"); time.sleep(1); st.rerun()

if st.button("▶️ CHẠY NGAY"):
    st.info("Đang xử lý...")
    # Lấy dòng Active của Block này (Dùng df_config gốc để có Token thật)
    rows_run = df_config[(df_config[logic.COL_BLOCK] == block) & (df_config[logic.COL_STATUS] == "Active")]
    
    for i, row in rows_run.iterrows():
        ok, msg, count = logic.sync_data(creds, row)
        if ok: st.toast(f"✅ {row[logic.COL_URL]}: +{count} dòng")
        else: st.error(f"❌ {row[logic.COL_URL]}: {msg}")
