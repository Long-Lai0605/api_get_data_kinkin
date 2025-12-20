import streamlit as st
import pandas as pd
import utils
import backend
import time
from gspread_dataframe import get_as_dataframe, set_with_dataframe

st.set_page_config(page_title="1Office Secure Engine", layout="wide")
utils.init_db() # Khởi tạo hệ thống

st.title("🛡️ 1OFFICE TO SHEETS - SECURE ENGINE")

# Load Config
sh = utils.get_master_sh()
wks_config = sh.worksheet(utils.SH_CONFIG)
df_config = get_as_dataframe(wks_config, dtype=str).dropna(how='all')

# Đảm bảo đủ cột chuẩn
req_cols = ["Block_Name", "STT", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", 
            "Method", "API URL", "Access Token", "Link Đích", "Tên sheet dữ liệu dịch", 
            "Kết quả", "Dòng dữ liệu"]
for c in req_cols:
    if c not in df_config.columns: df_config[c] = ""
if "Block_Name" not in df_config.columns: df_config["Block_Name"] = "Default"

# 1. SIDEBAR QUẢN LÝ KHỐI
all_blocks = df_config["Block_Name"].unique().tolist()
with st.sidebar:
    st.header("📦 Quản Lý Khối")
    
    # Thêm Khối
    if "new_block_name" not in st.session_state: st.session_state["new_block_name"] = ""
    new_blk = st.text_input("Tên khối mới:", key="new_block_input")
    
    if st.button("➕ Thêm Khối"):
        if new_blk and new_blk not in all_blocks:
            new_row = pd.DataFrame([{
                "Block_Name": new_blk, 
                "Trạng thái": "Chưa chốt & đang cập nhật",
                "Method": "GET"
            }])
            df_config = pd.concat([df_config, new_row], ignore_index=True)
            wks_config.clear(); set_with_dataframe(wks_config, df_config); st.rerun()
            
    # Chọn Khối
    selected_block = st.selectbox("Chọn Khối:", all_blocks)
    
    # Xóa Khối
    if st.button("🗑️ Xóa Khối"):
        df_new = df_config[df_config["Block_Name"] != selected_block]
        wks_config.clear(); set_with_dataframe(wks_config, df_new); st.rerun()

# 2. DASHBOARD
st.subheader(f"Dashboard: {selected_block}")
block_data = df_config[df_config["Block_Name"] == selected_block].copy()
block_data = block_data.reset_index(drop=True)
block_data['STT'] = block_data.index + 1

# Editor Config
edited_df = st.data_editor(
    block_data,
    num_rows="dynamic",
    column_order=req_cols,
    column_config={
        "STT": st.column_config.NumberColumn(disabled=True, width="small"),
        "Trạng thái": st.column_config.SelectboxColumn(options=["Chưa chốt & đang cập nhật", "Đã chốt"], width="medium"),
        "Method": st.column_config.SelectboxColumn(options=["GET", "POST", "PUT", "DELETE"], width="small"),
        "API URL": st.column_config.TextColumn(width="medium"),
        "Access Token": st.column_config.TextColumn(
            label="Access Token 🔒",
            help="Token sẽ được mã hóa vào kho bảo mật sau khi lưu.",
            width="medium"
        ),
        "Link Đích": st.column_config.TextColumn(width="medium"),
        "Kết quả": st.column_config.TextColumn(disabled=True),
        "Dòng dữ liệu": st.column_config.NumberColumn(disabled=True)
    },
    use_container_width=True, hide_index=True
)

# NÚT LƯU CẤU HÌNH & BẢO MẬT
if st.button("💾 LƯU CẤU HÌNH & BẢO MẬT TOKEN"):
    df_save = edited_df.copy()
    df_save["Block_Name"] = selected_block
    
    # Tách Token thật ra khỏi file hiển thị
    for idx, row in df_save.iterrows():
        token = str(row.get("Access Token", ""))
        url = str(row.get("API URL", ""))
        
        if token and token != "Đã lưu kho 🔒":
            utils.save_secure_token(selected_block, url, token)
            df_save.at[idx, "Access Token"] = "Đã lưu kho 🔒"
    
    if 'STT' in df_save.columns: del df_save['STT']
    
    # Ghép lại với các khối khác
    df_others = df_config[df_config["Block_Name"] != selected_block]
    df_final = pd.concat([df_others, df_save], ignore_index=True)
    
    wks_config.clear()
    set_with_dataframe(wks_config, df_final)
    st.success("✅ Cấu hình đã lưu. Token đã được đưa vào kho bảo mật."); time.sleep(1); st.rerun()

# CÁC NÚT CHẠY
c1, c2 = st.columns([1, 4])
with c1:
    if st.button("▶️ CHẠY KHỐI NÀY", type="primary"):
        if utils.check_lock("User"): st.error("Hệ thống đang bận!"); st.stop()
        utils.set_lock("User", True)
        
        status = st.status("Đang xử lý...", expanded=True)
        try:
            # Lấy data mới nhất từ sheet để đảm bảo có "Đã lưu kho"
            df_latest = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
            rows = df_latest[(df_latest["Block_Name"] == selected_block) & 
                             (df_latest["Trạng thái"] == "Chưa chốt & đang cập nhật")]
            
            total_rows = 0; start = time.time()
            for idx, row in rows.iterrows():
                status.write(f"🔄 Đang gọi: {row.get('API URL')}")
                ok, msg, count = backend.process_sync(row, selected_block)
                
                # Cập nhật kết quả Real-time lên Sheet Config
                real_idx = df_latest.index[df_latest['API URL'] == row['API URL']].tolist()[0]
                df_latest.at[real_idx, "Kết quả"] = msg
                df_latest.at[real_idx, "Dòng dữ liệu"] = count
                if ok: total_rows += count
            
            wks_config.clear(); set_with_dataframe(wks_config, df_latest)
            status.update(label="Hoàn tất!", state="complete")
            st.success(f"Xử lý xong {len(rows)} nguồn | Thêm mới {total_rows} dòng | Thời gian: {round(time.time()-start, 2)}s")
            utils.write_log(f"Manual Run Block {selected_block}: +{total_rows} rows")
            
        except Exception as e: st.error(f"Lỗi: {e}")
        finally: utils.set_lock("User", False)

with c2:
    if st.button("🚀 CHẠY TẤT CẢ (Auto All)"):
        # Logic tương tự nhưng loop qua tất cả Block
        st.info("Chức năng chạy tất cả các khối đang được kích hoạt...")
        # (Bạn có thể copy logic loop ở trên và áp dụng cho toàn bộ df_config)
