import streamlit as st
import pandas as pd
import utils
import backend
import time
from gspread_dataframe import get_as_dataframe, set_with_dataframe

st.set_page_config(page_title="Secure 1Office Tool", layout="wide")
utils.init_db() 

st.title("🚀 SECURE 1OFFICE AUTOMATION")

# Load Config
sh = utils.get_master_sh()
wks_config = sh.worksheet(utils.SH_CONFIG)
df_config = get_as_dataframe(wks_config, dtype=str).dropna(how='all')

# Đảm bảo đủ cột nếu file cũ chưa có
required_cols = ["Block_Name", "STT", "Trạng thái", "Ngày chốt", "Tháng", "Method", "API URL", "Access Token", "Link Đích", "Tên sheet dữ liệu dịch", "Kết quả", "Dòng dữ liệu"]
for col in required_cols:
    if col not in df_config.columns: df_config[col] = ""

if "Block_Name" not in df_config.columns: df_config["Block_Name"] = "Default"

# 1. SIDEBAR
all_blocks = df_config["Block_Name"].unique().tolist()
with st.sidebar:
    st.header("📦 Quản Lý Khối")
    new_blk = st.text_input("Thêm khối mới:")
    if st.button("➕ Thêm"):
        if new_blk and new_blk not in all_blocks:
            new_row = pd.DataFrame([{
                "Block_Name": new_blk, 
                "Trạng thái": "Chưa chốt & đang cập nhật",
                "Method": "GET"
            }])
            df_config = pd.concat([df_config, new_row], ignore_index=True)
            wks_config.clear(); set_with_dataframe(wks_config, df_config); st.rerun()
            
    selected_block = st.selectbox("Chọn Khối:", all_blocks)
    if st.button("🗑️ Xóa Khối"):
        df_new = df_config[df_config["Block_Name"] != selected_block]
        wks_config.clear(); set_with_dataframe(wks_config, df_new); st.rerun()

# 2. DASHBOARD
st.subheader(f"Cấu hình: {selected_block}")
block_data = df_config[df_config["Block_Name"] == selected_block].copy()

# Auto STT
block_data = block_data.reset_index(drop=True)
block_data['STT'] = block_data.index + 1

# --- HIỂN THỊ ĐỦ CỘT ---
edited_df = st.data_editor(
    block_data,
    num_rows="dynamic",
    column_order=[
        "STT", "Trạng thái", "Ngày chốt", "Tháng", 
        "Method", "API URL", "Access Token", 
        "Link Đích", "Tên sheet dữ liệu dịch", "Kết quả", "Dòng dữ liệu"
    ],
    column_config={
        "STT": st.column_config.NumberColumn(disabled=True, width="small"),
        "Trạng thái": st.column_config.SelectboxColumn(options=["Chưa chốt & đang cập nhật", "Đã chốt"], width="medium"),
        "Ngày chốt": st.column_config.TextColumn(width="small"),
        "Tháng": st.column_config.TextColumn(width="small"),
        "Method": st.column_config.SelectboxColumn(options=["GET", "POST"], width="small"),
        "API URL": st.column_config.TextColumn(width="medium", help="Endpoint 1Office"),
        "Access Token": st.column_config.TextColumn(label="Access Token 🔒", width="medium"),
        "Link Đích": st.column_config.TextColumn(width="medium"),
        "Tên sheet dữ liệu dịch": st.column_config.TextColumn(width="medium"),
        "Kết quả": st.column_config.TextColumn(disabled=True),
        "Dòng dữ liệu": st.column_config.NumberColumn(disabled=True)
    },
    use_container_width=True, hide_index=True
)

# NÚT LƯU
if st.button("💾 LƯU CẤU HÌNH & BẢO MẬT TOKEN"):
    df_save = edited_df.copy()
    df_save["Block_Name"] = selected_block
    
    # Token Logic
    for idx, row in df_save.iterrows():
        token = str(row.get("Access Token", ""))
        url = str(row.get("API URL", ""))
        if token and token != "Đã lưu kho 🔒":
            utils.save_secure_token(selected_block, url, token)
            df_save.at[idx, "Access Token"] = "Đã lưu kho 🔒"
            
    if 'STT' in df_save.columns: del df_save['STT']

    df_others = df_config[df_config["Block_Name"] != selected_block]
    df_final = pd.concat([df_others, df_save], ignore_index=True)
    
    wks_config.clear()
    set_with_dataframe(wks_config, df_final)
    st.success("✅ Đã lưu!"); time.sleep(1); st.rerun()

# NÚT CHẠY
if st.button("▶️ CHẠY KHỐI NÀY"):
    utils.set_lock("User", True)
    try:
        df_latest = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
        rows = df_latest[(df_latest["Block_Name"] == selected_block) & 
                         (df_latest["Trạng thái"] == "Chưa chốt & đang cập nhật")]
        
        status = st.status("Đang chạy...", expanded=True)
        total = 0
        for i, row in rows.iterrows():
            status.write(f"🔄 {row.get('API URL')}")
            ok, msg, count = backend.process_sync(row, selected_block)
            
            # Update Realtime
            idx_real = df_latest.index[df_latest['API URL'] == row['API URL']].tolist()[0]
            df_latest.at[idx_real, "Kết quả"] = msg
            df_latest.at[idx_real, "Dòng dữ liệu"] = count
            if ok: total += count
        
        wks_config.clear(); set_with_dataframe(wks_config, df_latest)
        status.update(label="Xong!", state="complete")
        st.success(f"Thêm mới: {total} dòng")
    finally: utils.set_lock("User", False)
