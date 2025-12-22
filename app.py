import streamlit as st
import utils
import pandas as pd
import time
from datetime import datetime

st.set_page_config(page_title="1OFFICE ENGINE", layout="wide", page_icon="🛡️")

# CSS tùy chỉnh giao diện
st.markdown("""
<style>
    .stProgress > div > div > div > div { background-color: #00cc00; }
    .status-box { padding: 10px; border-radius: 5px; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# Khởi tạo DB ngay khi vào app
try:
    utils.init_db()
except:
    st.warning("Đang khởi tạo kết nối...")

st.title("🛡️ 1OFFICE TO SHEETS - MULTI-BLOCK ENGINE")

# --- TABS ---
tab1, tab2 = st.tabs(["🚀 Dashboard & Vận hành", "⚙️ Thêm Khối Mới"])

# === TAB 1: DASHBOARD ===
with tab1:
    st.subheader("Trạng thái hệ thống")
    
    # Lấy dữ liệu
    blocks = utils.get_all_blocks_secure()
    
    if not blocks:
        st.info("Hệ thống chưa có khối dữ liệu nào.")
    else:
        # Chuyển DF để hiển thị
        df = pd.DataFrame(blocks)
        
        # --- SECURITY MASKING ---
        # Ẩn cột Token thật, thay bằng text khóa
        if 'Access Token (Encrypted)' in df.columns:
            df['Access Token (Encrypted)'] = "Đã lưu kho 🔒"
            
        # Chọn cột hiển thị
        display_cols = ["Block Name", "Trạng thái", "Method", "API URL", "Access Token (Encrypted)", "Total Rows", "Last Run", "Kết quả"]
        # Lọc cột tồn tại
        final_cols = [c for c in display_cols if c in df.columns]
        
        st.dataframe(df[final_cols], use_container_width=True)
        
        # --- NÚT ĐIỀU KHIỂN ---
        if st.button("▶️ CHẠY TẤT CẢ (RUN ALL)", type="primary"):
            st.divider()
            status_container = st.container()
            progress_bar = st.progress(0)
            
            total_blocks = len(blocks)
            processed_count = 0
            total_new_rows = 0
            start_time = time.time()
            
            for i, block in enumerate(blocks):
                # Chỉ chạy khối 'Chưa chốt'
                status = block.get('Trạng thái', '')
                if "Đã chốt" in status:
                    continue
                    
                b_name = block['Block Name']
                
                with status_container:
                    with st.spinner(f"Đang xử lý khối: {b_name}..."):
                        # Gọi hàm xử lý
                        success, msg, rows = utils.run_single_block(block)
                        
                        if success:
                            st.toast(f"✅ {b_name}: +{rows} dòng", icon="✅")
                            total_new_rows += rows
                            processed_count += 1
                        else:
                            st.error(f"❌ {b_name}: {msg}")
                
                # Cập nhật tiến độ
                progress_bar.progress((i + 1) / total_blocks)
            
            end_time = time.time()
            duration = round(end_time - start_time, 2)
            
            st.success(f"""
            🎉 **HOÀN TẤT QUÁ TRÌNH!**
            - Số nguồn xử lý: {processed_count}
            - Tổng dòng thêm mới: {total_new_rows}
            - Thời gian: {duration} giây
            """)

# === TAB 2: THÊM KHỐI MỚI ===
with tab2:
    st.markdown("### Cấu hình Khối Dữ liệu (Block)")
    with st.form("add_block_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên Khối (Bắt buộc)", placeholder="VD: NS_Thang12")
        method = c2.selectbox("Method", ["GET", "POST"])
        
        url = st.text_input("API URL", placeholder="https://kinkin.1office.vn/api/...")
        token = st.text_input("Access Token (Sẽ được mã hóa)", type="password")
        
        c3, c4 = st.columns(2)
        link = c3.text_input("Link Sheet Đích")
        sheet_name = c4.text_input("Tên Sheet Đích")
        
        c5, c6 = st.columns(2)
        d_start = c5.date_input("Ngày bắt đầu")
        d_end = c6.date_input("Ngày kết thúc")
        
        submitted = st.form_submit_button("Lưu cấu hình & Token")
        
        if submitted:
            if not name or not url or not token:
                st.error("Vui lòng nhập Tên khối, URL và Token!")
            else:
                try:
                    utils.add_new_block_secure(name, method, url, token, link, sheet_name, d_start, d_end)
                    st.success(f"Đã thêm khối '{name}' thành công. Token đã được cất vào kho bảo mật.")
                except Exception as e:
                    st.error(f"Lỗi khi lưu: {e}")
