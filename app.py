import streamlit as st
import utils
import pandas as pd
import time

# --- SETUP TRANG ---
st.set_page_config(page_title="1OFFICE ENGINE", layout="wide", page_icon="🛡️")

# CSS Custom
st.markdown("""
<style>
    .stButton>button { width: 100%; font-weight: bold; }
    .status-ok { color: green; font-weight: bold; }
    .status-err { color: red; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Khởi tạo DB khi vào App
with st.spinner("Đang kết nối hệ thống..."):
    utils.init_db()

st.title("🛡️ 1OFFICE MULTI-BLOCK ENGINE")
st.caption("Hệ thống đồng bộ dữ liệu bảo mật từ 1Office về Google Sheets")

# --- TABS GIAO DIỆN ---
tab_dash, tab_add = st.tabs(["🚀 Dashboard Quản Lý", "➕ Thêm Khối Mới"])

# ==========================================
# TAB 1: DASHBOARD & RUN
# ==========================================
with tab_dash:
    # 1. Load dữ liệu
    blocks = utils.get_all_blocks_secure()
    
    if not blocks:
        st.info("Chưa có cấu hình nào. Vui lòng sang Tab 'Thêm Khối Mới'.")
    else:
        # Hiển thị DataFrame với Token được Masking
        df_show = pd.DataFrame(blocks)
        
        # MASKING TOKEN (Mục II.1)
        if 'Access Token (Encrypted)' in df_show.columns:
            df_show['Access Token (Encrypted)'] = "Đã lưu kho 🔒"
            
        # Chọn cột hiển thị
        cols = ["Block Name", "Trạng thái", "Method", "API URL", "Access Token (Encrypted)", "Link Đích", "Sheet Đích", "Total Rows", "Last Run"]
        # Lọc cột tồn tại
        valid_cols = [c for c in cols if c in df_show.columns]
        
        st.dataframe(df_show[valid_cols], use_container_width=True)
        
        st.divider()
        
        # NÚT CHẠY TẤT CẢ (Mục I)
        if st.button("▶️ CHẠY TẤT CẢ CÁC KHỐI", type="primary"):
            progress_bar = st.progress(0)
            status_box = st.empty()
            
            total = len(blocks)
            success_count = 0
            total_rows_added = 0
            start_time = time.time()
            
            for i, block in enumerate(blocks):
                b_name = block['Block Name']
                
                # Chỉ chạy khối "Chưa chốt"
                if "Đã chốt" in block.get("Trạng thái", ""):
                    continue
                
                status_box.markdown(f"⏳ **Đang xử lý khối:** `{b_name}`...")
                
                # 1. Gọi API (Logic VI)
                data, msg = utils.call_1office_api_logic_v6(
                    block['API URL'], 
                    block['Access Token (Encrypted)'], 
                    block['Method']
                )
                
                if msg == "Hết hạn API":
                    st.toast(f"❌ {b_name}: Token hết hạn!", icon="⛔")
                elif not data:
                    st.toast(f"⚠️ {b_name}: Không có dữ liệu.", icon="⚠️")
                else:
                    # 2. Ghi Sheet (Logic III)
                    rows, save_msg = utils.process_and_save_data(block, data)
                    
                    if "Lỗi" in save_msg:
                        st.error(f"{b_name}: {save_msg}")
                    else:
                        st.toast(f"✅ {b_name}: +{rows} dòng", icon="✅")
                        success_count += 1
                        total_rows_added += rows
                        
                # Update Progress
                progress_bar.progress((i + 1) / total)
            
            end_time = time.time()
            duration = round(end_time - start_time, 2)
            
            status_box.success(f"""
            🎉 **HOÀN TẤT!**
            - Xử lý xong: {success_count}/{total} nguồn
            - Thêm mới: {total_rows_added} dòng
            - Thời gian: {duration} giây
            """)

# ==========================================
# TAB 2: THÊM KHỐI MỚI (INPUT FORM)
# ==========================================
with tab_add:
    st.markdown("### Thiết lập cấu hình nguồn dữ liệu mới")
    
    with st.form("new_block_form", clear_on_submit=True): # Reset form sau khi submit
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên Khối (Block Name) *", placeholder="VD: NhanSu_T12")
        method = c2.selectbox("Method API", ["GET", "POST"])
        
        url = st.text_input("API URL *", placeholder="https://kinkin.1office.vn/api/...")
        token = st.text_input("Access Token *", type="password", help="Token sẽ được mã hóa vào sheet riêng")
        
        c3, c4 = st.columns(2)
        link_dest = c3.text_input("Link Sheet Đích *")
        sheet_dest = c4.text_input("Tên Sheet Đích *")
        
        c5, c6 = st.columns(2)
        d_start = c5.date_input("Ngày bắt đầu")
        d_end = c6.date_input("Ngày kết thúc")
        
        submitted = st.form_submit_button("Lưu Cấu Hình")
        
        if submitted:
            if not name or not url or not token or not link_dest:
                st.error("Vui lòng điền các trường bắt buộc (*)")
            else:
                try:
                    utils.add_new_block(name, method, url, token, link_dest, sheet_dest, d_start, d_end)
                    st.success(f"✅ Đã thêm khối '{name}'. Token đã được lưu bảo mật.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Lỗi: {e}")
