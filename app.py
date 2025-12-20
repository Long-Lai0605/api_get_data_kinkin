import streamlit as st
import pandas as pd
import utils
import backend
import time
from gspread_dataframe import get_as_dataframe, set_with_dataframe

st.set_page_config(page_title="Secure 1Office Tool", layout="wide")
utils.init_db() # Khởi tạo các sheet hệ thống

st.title("🚀 SECURE 1OFFICE AUTOMATION")

# Load Config
sh = utils.get_master_sh()
wks_config = sh.worksheet(utils.SH_CONFIG)
df_config = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
if "Block_Name" not in df_config.columns: df_config["Block_Name"] = "Default"

# 1. QUẢN LÝ KHỐI
all_blocks = df_config["Block_Name"].unique().tolist()
with st.sidebar:
    st.header("📦 Quản Lý Khối")
    new_blk = st.text_input("Thêm khối mới:")
    if st.button("➕ Thêm"):
        if new_blk and new_blk not in all_blocks:
            new_row = pd.DataFrame([{"Block_Name": new_blk, "Trạng thái": "Chưa chốt & đang cập nhật"}])
            df_config = pd.concat([df_config, new_row], ignore_index=True)
            wks_config.clear(); set_with_dataframe(wks_config, df_config); st.rerun()
            
    selected_block = st.selectbox("Chọn Khối:", all_blocks)
    if st.button("🗑️ Xóa Khối"):
        df_new = df_config[df_config["Block_Name"] != selected_block]
        wks_config.clear(); set_with_dataframe(wks_config, df_new); st.rerun()

# 2. DASHBOARD CẤU HÌNH
st.subheader(f"Cấu hình: {selected_block}")
block_data = df_config[df_config["Block_Name"] == selected_block].copy()

edited_df = st.data_editor(
    block_data,
    num_rows="dynamic",
    column_config={
        "STT": st.column_config.NumberColumn(disabled=True),
        "Trạng thái": st.column_config.SelectboxColumn(options=["Chưa chốt & đang cập nhật", "Đã chốt"]),
        "Method": st.column_config.SelectboxColumn(options=["GET", "POST", "PUT", "DELETE"]),
        "Access Token": st.column_config.TextColumn(
            label="Access Token (Bảo mật)",
            help="Nhập Token thật vào đây. Sau khi Lưu, hệ thống sẽ ẩn đi.",
            width="large"
        ),
        "Kết quả": st.column_config.TextColumn(disabled=True),
        "Dòng dữ liệu": st.column_config.NumberColumn(disabled=True)
    },
    use_container_width=True, hide_index=True
)

if st.button("💾 LƯU CẤU HÌNH & BẢO MẬT TOKEN"):
    # Logic: Tách Token thật ra lưu riêng
    df_to_display = edited_df.copy()
    df_to_display["Block_Name"] = selected_block
    
    for idx, row in df_to_display.iterrows():
        token_input = str(row.get("Access Token", ""))
        url = str(row.get("API URL", ""))
        
        # Nếu user nhập token mới (khác 'Đã lưu kho' và không rỗng)
        if token_input and token_input != "Đã lưu kho 🔒":
            # A. Lưu Token thật vào sheet bảo mật
            utils.save_secure_token(selected_block, url, token_input)
            # B. Thay thế trên UI bằng mặt nạ
            df_to_display.at[idx, "Access Token"] = "Đã lưu kho 🔒"
            
    # Lưu lại Config Sheet
    df_others = df_config[df_config["Block_Name"] != selected_block]
    df_final = pd.concat([df_others, df_to_display], ignore_index=True)
    
    wks_config.clear()
    set_with_dataframe(wks_config, df_final)
    st.success("Đã lưu cấu hình và mã hóa Token!")
    time.sleep(1); st.rerun()

# 3. CHẠY THỦ CÔNG
if st.button("▶️ CHẠY KHỐI NÀY"):
    if utils.check_lock("User"): st.error("Hệ thống đang bận!"); st.stop()
    utils.set_lock("User", True)
    
    st_status = st.status("Đang xử lý...", expanded=True)
    try:
        # Lấy lại data mới nhất từ sheet
        df_run = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
        rows_run = df_run[(df_run["Block_Name"] == selected_block) & 
                          (df_run["Trạng thái"] == "Chưa chốt & đang cập nhật")]
        
        total_src = 0; total_new = 0; start = time.time()
        
        for idx, row in rows_run.iterrows():
            st_status.write(f"🔄 Đang xử lý: {row['API URL']}")
            ok, msg, count = backend.process_sync(row, selected_block)
            
            # Update kết quả vào Config
            real_idx = df_run.index[df_run['API URL'] == row['API URL']].tolist()[0]
            df_run.at[real_idx, "Kết quả"] = msg
            df_run.at[real_idx, "Dòng dữ liệu"] = count
            
            if ok: total_src += 1; total_new += count
        
        wks_config.clear()
        set_with_dataframe(wks_config, df_run)
        
        elapsed = round(time.time() - start, 2)
        st_status.update(label="Hoàn tất!", state="complete")
        st.success(f"Xử lý xong {total_src} nguồn | Thêm mới {total_new} dòng | Thời gian: {elapsed}s")
        utils.write_log(f"Manual Run: {selected_block} - +{total_new} rows")

    except Exception as e: st.error(f"Lỗi: {e}")
    finally: utils.set_lock("User", False)
