import streamlit as st
import pandas as pd
import utils
import backend
import time
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# 1. CẤU HÌNH TRANG
st.set_page_config(page_title="1Office Secure Engine", layout="wide")
utils.init_db() # Khởi tạo hệ thống database

st.title("🛡️ 1OFFICE TO SHEETS - SECURE ENGINE")

# 2. LOAD CẤU HÌNH TỪ GOOGLE SHEET
try:
    sh = utils.get_master_sh()
    wks_config = sh.worksheet(utils.SH_CONFIG)
    df_config = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
except Exception as e:
    st.error(f"Lỗi kết nối Google Sheet: {e}")
    st.stop()

# Đảm bảo đủ cột chuẩn
req_cols = ["Block_Name", "STT", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", 
            "Method", "API URL", "Access Token", "Link Đích", "Tên sheet dữ liệu dịch", 
            "Kết quả", "Dòng dữ liệu"]
for c in req_cols:
    if c not in df_config.columns: df_config[c] = ""
if "Block_Name" not in df_config.columns: df_config["Block_Name"] = "Default"

# 3. SIDEBAR QUẢN LÝ KHỐI
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

# 4. DASHBOARD HIỂN THỊ
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

# 5. NÚT LƯU CẤU HÌNH & BẢO MẬT
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

# 6. KHU VỰC CHẠY (RUN AREA)
st.divider()
c1, c2 = st.columns([1, 4])

with c1:
    # --- NÚT CHẠY VỚI LIVE STATUS LOG ---
    if st.button("▶️ CHẠY KHỐI NÀY", type="primary"):
        if utils.check_lock("User"): st.error("Hệ thống đang bận!"); st.stop()
        utils.set_lock("User", True)
        
        # Container trạng thái
        status_box = st.status("🚀 Đang khởi động...", expanded=True)
        
        try:
            # Hàm callback cập nhật UI
            def ui_logger(msg):
                status_box.write(msg)
                time.sleep(0.05)

            # Lấy data config mới nhất
            df_latest = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
            rows_run = df_latest[(df_latest["Block_Name"] == selected_block) & 
                                 (df_latest["Trạng thái"] == "Chưa chốt & đang cập nhật")]
            
            total_rows = 0; start = time.time()
            
            if rows_run.empty:
                status_box.update(label="⚠️ Không có dòng nào 'Chưa chốt' để chạy!", state="error")
                st.warning("Vui lòng kiểm tra lại trạng thái các dòng cấu hình.")
            else:
                for idx, row in rows_run.iterrows():
                    api_url = row.get('API URL', 'Unknown URL')
                    status_box.write(f"🔵 **Đang xử lý nguồn:** `{api_url}`")
                    
                    # Gọi Backend kèm Callback
                    ok, msg, count = backend.process_sync(row, selected_block, callback=ui_logger)
                    
                    # Update kết quả
                    if ok:
                        status_box.write(f"✅ **Xong nguồn này:** +{count} dòng.")
                        total_rows += count
                    else:
                        status_box.write(f"❌ **Lỗi:** {msg}")
                    
                    # Lưu vào DB Real-time
                    real_idx = df_latest.index[df_latest['API URL'] == api_url].tolist()[0]
                    df_latest.at[real_idx, "Kết quả"] = msg
                    df_latest.at[real_idx, "Dòng dữ liệu"] = count
                
                # Lưu config cuối cùng
                wks_config.clear()
                set_with_dataframe(wks_config, df_latest)
                
                elapsed = round(time.time() - start, 2)
                status_box.update(label="🎉 Hoàn tất quy trình!", state="complete", expanded=False)
                
                if total_rows > 0:
                    st.success(f"📊 Tổng kết: Thêm mới {total_rows} dòng | Thời gian: {elapsed}s")
                else:
                    st.warning(f"⚠️ Chạy xong nhưng không có dữ liệu nào được thêm. (Thời gian: {elapsed}s)")

        except Exception as e:
            st.error(f"🔥 Lỗi nghiêm trọng: {e}")
        finally:
            utils.set_lock("User", False)

with c2:
    if st.button("🚀 CHẠY TẤT CẢ (Auto All)"):
        st.info("Tính năng đang phát triển...")
