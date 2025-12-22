import streamlit as st
import backend as be
import pandas as pd
import time

st.set_page_config(page_title="KINKIN ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

st.title("⚡ 1OFFICE PARALLEL ENGINE")
tab1, tab2 = st.tabs(["🚀 Dashboard", "➕ Thêm Khối Mới"])

# --- TAB 1: DASHBOARD ---
with tab1:
    blocks = be.get_active_blocks(st.secrets)
    
    if not blocks:
        st.info("Chưa có cấu hình nào.")
    else:
        df = pd.DataFrame(blocks)
        if 'Access Token (Encrypted)' in df.columns:
            df['Access Token (Encrypted)'] = "Đã lưu kho 🔒"

        df.rename(columns={"Total Rows": "Dải dòng dữ liệu", "Last Run": "Cập nhật cuối"}, inplace=True)
        st.dataframe(df, use_container_width=True)
        
        if st.button("▶️ CHẠY TẤT CẢ", type="primary"):
            for i, block in enumerate(blocks):
                b_name = block['Block Name']
                if "Đã chốt" in block.get("Trạng thái", ""): continue

                with st.status(f"🔄 **{b_name}**", expanded=True) as status:
                    def update_text(t): status.write(t)
                    
                    # 1. Lấy thông tin cấu hình
                    token = block.get('Access Token (Encrypted)', '')
                    f_key = block.get("Filter Key", "")
                    d_s_str = block.get("Ngày bắt đầu", "")
                    d_e_str = block.get("Ngày kết thúc", "")
                    
                    # Convert ngày tháng
                    d_s_obj = pd.to_datetime(d_s_str, dayfirst=False).date() if d_s_str else None
                    d_e_obj = pd.to_datetime(d_e_str, dayfirst=False).date() if d_e_str else None

                    # 2. GỌI HÀM MỚI (SMART FETCH)
                    # [FIX]: Đổi tên hàm từ fetch_1office_data_parallel -> fetch_1office_data_smart
                    data, msg = be.fetch_1office_data_smart(
                        url=block.get('API URL', ''), 
                        token=token, 
                        method=block.get('Method', 'GET'), 
                        filter_key=f_key,
                        date_start=d_s_obj,
                        date_end=d_e_obj,
                        status_callback=update_text
                    )

                    if msg.startswith("Success") and data is not None:
                        status.write(f"✅ Tải xong {len(data)} dòng từ Server.")
                        
                        # 3. Ghi Sheet (Đã có Auto Header)
                        range_str, w_msg = be.write_to_sheet_range(st.secrets, block, data)
                        
                        if "Error" not in w_msg:
                            status.update(label=f"✅ {b_name}: Hoàn tất! (+{len(data)})", state="complete", expanded=False)
                            st.toast(f"✅ {b_name}: +{len(data)} dòng")
                        else:
                            status.update(label=f"❌ {b_name}: Lỗi ghi sheet", state="error")
                            st.error(w_msg)
                            
                    elif msg == "Hết hạn API":
                        status.update(label=f"⛔ {b_name}: Token hết hạn!", state="error")
                    else:
                        status.update(label=f"⚠️ {b_name}: {msg}", state="error")
            
            st.success("Đã chạy xong!")
            time.sleep(1.5)
            st.rerun()

# --- TAB 2: THÊM MỚI ---
with tab2:
    st.markdown("### Cấu hình Khối mới")
    with st.form("add_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên Khối (Block Name) *")
        method = c2.selectbox("Method", ["GET", "POST"])
        
        url = st.text_input("API URL *")
        token = st.text_input("Token *", type="password")
        
        c3, c4 = st.columns(2)
        link = c3.text_input("Link Sheet Đích *")
        sheet = c4.text_input("Tên Sheet Đích *")
        
        st.divider()
        st.markdown("**Bộ lọc Dữ liệu (Filter)**")
        st.caption("Nhập tên trường dữ liệu trong API (VD: `created_date`) để lọc theo khoảng ngày bên dưới.")
        
        filter_key = st.text_input("Trường dữ liệu cần lọc (Key)", placeholder="VD: created_date")
        
        col_d1, col_d2 = st.columns(2)
        start = col_d1.date_input("Ngày bắt đầu")
        end = col_d2.date_input("Ngày kết thúc")
        
        submitted = st.form_submit_button("Lưu & Cập nhật Dashboard")
        
        if submitted:
            if not name or not url or not token or not link:
                st.error("Thiếu thông tin bắt buộc!")
            else:
                ok = be.add_new_block(st.secrets, name, method, url, token, link, sheet, start, end, filter_key)
                if ok:
                    st.toast("✅ Đã thêm thành công!")
                    time.sleep(1)
                    st.rerun()
