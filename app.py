import streamlit as st
import utils
import pandas as pd
import time

st.set_page_config(page_title="KINKIN AUTOMATION ENGINE", layout="wide", page_icon="🛡️")

# --- CSS TÙY CHỈNH ---
st.markdown("""
<style>
    .stButton>button {width: 100%; border-radius: 5px;}
    .reportview-container {background: #f0f2f6;}
    .success-status {color: green; font-weight: bold;}
    .error-status {color: red; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

# Khởi tạo DB
utils.init_db()

st.title("🛡️ 1OFFICE TO SHEETS - MULTI-BLOCK ENGINE")

# --- TAB QUẢN LÝ ---
tab1, tab2 = st.tabs(["🚀 Dashboard & Điều khiển", "⚙️ Cấu hình Khối (Blocks)"])

# === TAB 1: DASHBOARD ===
with tab1:
    st.subheader("Trạng thái các luồng dữ liệu")
    
    # Load dữ liệu từ Master Sheet
    blocks = utils.get_all_blocks()
    
    if not blocks:
        st.info("Chưa có khối dữ liệu nào. Vui lòng sang tab Cấu hình để thêm.")
    else:
        # Chuyển thành DataFrame để hiển thị
        df_display = pd.DataFrame(blocks)
        
        # [SECURITY MASKING] Ẩn Token trên giao diện
        if 'Access Token (Encrypted)' in df_display.columns:
            df_display['Access Token (Encrypted)'] = "Đã lưu kho 🔒"
            
        # Chọn các cột cần hiển thị theo yêu cầu prompt
        cols_show = ["Block Name", "Trạng thái", "Method", "API URL", "Access Token (Encrypted)", "Total Rows", "Last Run"]
        # Lọc cột tồn tại để tránh lỗi
        valid_cols = [c for c in cols_show if c in df_display.columns]
        
        st.dataframe(df_display[valid_cols], use_container_width=True)
        
        # --- KHU VỰC ĐIỀU KHIỂN ---
        col_act1, col_act2 = st.columns([1, 4])
        with col_act1:
            if st.button("▶️ CHẠY TẤT CẢ", type="primary"):
                progress_bar = st.progress(0)
                status_log = st.empty()
                
                total_blocks = len(blocks)
                success_count = 0
                
                for i, block in enumerate(blocks):
                    # Chỉ chạy khối đang active
                    if "Đã chốt" in block.get('Trạng thái', ''):
                        continue
                        
                    status_log.text(f"⏳ Đang xử lý khối: {block['Block Name']}...")
                    
                    is_success, msg, rows = utils.run_block_process(block)
                    
                    if is_success:
                        st.toast(f"✅ {block['Block Name']}: +{rows} dòng", icon="✅")
                        success_count += 1
                    else:
                        st.toast(f"❌ {block['Block Name']}: {msg}", icon="ERROR")
                    
                    progress_bar.progress((i + 1) / total_blocks)
                    time.sleep(1) # Delay nhẹ tránh spam API
                
                status_log.success(f"🎉 Hoàn tất! Đã xử lý thành công {success_count}/{total_blocks} nguồn.")

# === TAB 2: CẤU HÌNH ===
with tab2:
    st.markdown("### Thêm Khối Dữ Liệu Mới")
    with st.form("add_block_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        block_name = c1.text_input("Tên Khối (Block Name)", placeholder="VD: NhanSu_Thang10")
        method = c2.selectbox("Method", ["POST", "GET"])
        
        api_url = st.text_input("API URL", placeholder="https://kinkin.1office.vn/api/...")
        
        # Input Token (Sẽ được ẩn sau khi lưu)
        token = st.text_input("Access Token (Lấy từ F12 Network)", type="password", help="Token sẽ được mã hóa và lưu vào sheet bảo mật")
        
        c3, c4 = st.columns(2)
        des_link = c3.text_input("Link Google Sheet Đích")
        des_sheet = c4.text_input("Tên Sheet Đích")
        
        c5, c6 = st.columns(2)
        start_date = c5.date_input("Ngày bắt đầu")
        end_date = c6.date_input("Ngày kết thúc")
        
        submitted = st.form_submit_button("Lưu cấu hình")
        
        if submitted:
            if not block_name or not api_url or not token:
                st.error("Vui lòng nhập đầy đủ thông tin bắt buộc!")
            else:
                with st.spinner("Đang lưu vào Master Sheet..."):
                    utils.add_new_block(block_name, method, api_url, token, des_link, des_sheet, start_date, end_date)
                    st.success(f"Đã thêm khối '{block_name}' thành công! Token đã được bảo mật.")
                    time.sleep(1)
                    st.rerun()

    st.warning("⚠️ Lưu ý: Để xóa khối, vui lòng truy cập trực tiếp Master Sheet (Sheet 'luu_cau_hinh') để đảm bảo an toàn dữ liệu.")
