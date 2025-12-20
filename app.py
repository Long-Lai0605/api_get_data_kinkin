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

# Đảm bảo có cột Block_Name
if "Block_Name" not in df_config.columns: df_config["Block_Name"] = "Default"

# 1. SIDEBAR QUẢN LÝ KHỐI
all_blocks = df_config["Block_Name"].unique().tolist()
with st.sidebar:
    st.header("📦 Quản Lý Khối")
    new_blk = st.text_input("Thêm khối mới:")
    if st.button("➕ Thêm"):
        if new_blk and new_blk not in all_blocks:
            # Tạo dòng mới với Header chuẩn
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

# 2. DASHBOARD CHÍNH
st.subheader(f"Cấu hình: {selected_block}")
block_data = df_config[df_config["Block_Name"] == selected_block].copy()

# Tự động đánh số STT nếu chưa có
block_data = block_data.reset_index(drop=True)
block_data['STT'] = block_data.index + 1

# Cấu hình hiển thị bảng
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
        "Method": st.column_config.SelectboxColumn(options=["GET", "POST", "PUT", "DELETE"], width="small"),
        "API URL": st.column_config.TextColumn(width="medium", help="Điền Endpoint API 1Office"),
        "Access Token": st.column_config.TextColumn(
            label="Access Token 🔒",
            help="Nhập Token thật. Sau khi Lưu sẽ tự động ẩn đi.",
            width="large"
        ),
        "Link Đích": st.column_config.TextColumn(width="medium"),
        "Kết quả": st.column_config.TextColumn(disabled=True),
        "Dòng dữ liệu": st.column_config.NumberColumn(disabled=True)
    },
    use_container_width=True, hide_index=True
)

# NÚT LƯU
if st.button("💾 LƯU CẤU HÌNH & BẢO MẬT TOKEN"):
    df_to_save = edited_df.copy()
    df_to_save["Block_Name"] = selected_block
    
    # Xử lý Token bảo mật
    for idx, row in df_to_save.iterrows():
        token_input = str(row.get("Access Token", ""))
        url = str(row.get("API URL", ""))
        
        if token_input and token_input != "Đã lưu kho 🔒":
            utils.save_secure_token(selected_block, url, token_input)
            df_to_save.at[idx, "Access Token"] = "Đã lưu kho 🔒"
            
    # Xóa cột STT tạm trước khi lưu vào DB
    if 'STT' in df_to_save.columns: del df_to_save['STT']

    # Lưu vào Sheet
    df_others = df_config[df_config["Block_Name"] != selected_block]
    df_final = pd.concat([df_others, df_to_save], ignore_index=True)
    
    wks_config.clear()
    set_with_dataframe(wks_config, df_final)
    st.success("✅ Đã lưu cấu hình!")
    time.sleep(1); st.rerun()

# NÚT CHẠY
if st.button("▶️ CHẠY KHỐI NÀY"):
    if utils.check_lock("User"): st.error("Hệ thống đang bận!"); st.stop()
    utils.set_lock("User", True)
    
    status = st.status("Đang xử lý...", expanded=True)
    try:
        # Lấy data mới nhất từ Sheet (để chắc chắn có Token đã lưu)
        df_latest = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
        rows_run = df_latest[(df_latest["Block_Name"] == selected_block) & 
                             (df_latest["Trạng thái"] == "Chưa chốt & đang cập nhật")]
        
        total_rows = 0; start = time.time()
        
        for idx, row in rows_run.iterrows():
            status.write(f"🔄 Đang gọi: {row.get('API URL', '')}")
            ok, msg, count = backend.process_sync(row, selected_block)
            
            # Cập nhật kết quả vào DB ngay lập tức (Real-time update)
            real_idx = df_latest.index[df_latest['API URL'] == row['API URL']].tolist()[0]
            df_latest.at[real_idx, "Kết quả"] = msg
            df_latest.at[real_idx, "Dòng dữ liệu"] = count
            if ok: total_rows += count
        
        # Lưu kết quả chạy
        wks_config.clear()
        set_with_dataframe(wks_config, df_latest)
        
        status.update(label="Hoàn tất!", state="complete")
        st.success(f"✅ Xong! Tổng dòng mới: {total_rows} | Thời gian: {round(time.time()-start, 2)}s")

    except Exception as e: st.error(f"Lỗi: {e}")
    finally: utils.set_lock("User", False)
