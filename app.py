import streamlit as st
import backend as be
import pandas as pd
import time
import json
import uuid
from datetime import time as dt_time

st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- INIT SESSION STATE ---
if 'view' not in st.session_state: st.session_state['view'] = 'list' # 'list' or 'detail'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""

with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- NAVIGATION FUNCTIONS ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# --- CORE RUN LOGIC ---
def run_link_process(link_data, block_name, status_container):
    url = link_data.get('API URL')
    token = link_data.get('Access Token')
    f_key = link_data.get('Filter Key')
    
    # Parse Date
    d_s_raw = link_data.get('Date Start')
    d_e_raw = link_data.get('Date End')
    d_s = pd.to_datetime(d_s_raw).date() if d_s_raw else None
    d_e = pd.to_datetime(d_e_raw).date() if d_e_raw else None
    
    def cb(msg): status_container.write(f"👉 {msg}")
    
    data, msg = be.fetch_1office_data_smart(url, token, 'GET', f_key, d_s, d_e, cb)
    
    if msg == "Success" and data:
        status_container.write(f"✅ Tải {len(data)} dòng. Ghi Sheet...")
        res, w_msg = be.write_to_sheet_range(
            st.secrets, 
            link_data.get('Link Sheet'), 
            link_data.get('Sheet Name'), 
            block_name, 
            data
        )
        if "Error" not in w_msg:
            return True, f"Xong! {res}"
        else: return False, f"Lỗi ghi: {w_msg}"
    return False, msg

# ==============================================================================
# VIEW 1: DANH SÁCH KHỐI (LIST VIEW)
# ==============================================================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    
    # Header & Add New
    col_a, col_b = st.columns([3, 1])
    with col_a:
        st.caption("Mỗi khối có thể chứa nhiều Link API và có lịch chạy riêng.")
    with col_b:
        with st.popover("➕ Thêm Khối Mới", use_container_width=True):
            new_b_name = st.text_input("Tên Khối (VD: Khối Nhân sự)")
            if st.button("Tạo Khối"):
                if new_b_name:
                    be.create_block(st.secrets, new_b_name)
                    st.success("Đã tạo!")
                    time.sleep(0.5)
                    st.rerun()

    # Load Blocks
    blocks = be.get_all_blocks(st.secrets)
    
    if not blocks:
        st.info("Chưa có khối nào. Hãy tạo khối mới.")
    else:
        # RUN ALL BUTTON
        if st.button("▶️ CHẠY TẤT CẢ CÁC KHỐI (PARALLEL)", type="primary"):
            st.toast("Đang khởi động chạy toàn bộ...")
            for b in blocks:
                st.write(f"🚀 Kích hoạt khối: **{b['Block Name']}**")
                links = be.get_links_by_block(st.secrets, b['Block ID'])
                for l in links:
                    if l.get("Status") == "Active":
                        with st.status(f"Run: {l.get('Sheet Name')}") as s:
                            run_link_process(l, b['Block Name'], s)

        st.divider()
        
        # Display Blocks as Cards
        for b in blocks:
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                c1.subheader(f"📦 {b['Block Name']}")
                c2.caption(f"Lịch: {b['Schedule Type']}")
                
                # Nút Chạy riêng Khối
                if c3.button("▶️ Chạy Khối", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    if not links:
                        st.warning("Khối này chưa có Link nào!")
                    else:
                        with st.status(f"Đang chạy {len(links)} link trong khối {b['Block Name']}...", expanded=True):
                            for l in links:
                                if l.get("Status") == "Active":
                                    st.write(f"**--- {l.get('Sheet Name')} ---**")
                                    ok, msg = run_link_process(l, b['Block Name'], st)
                                    if ok: st.success(msg)
                                    else: st.error(msg)
                
                # Nút Chi tiết & Xóa
                with c4:
                    if st.button("⚙️ Chi tiết", key=f"detail_{b['Block ID']}"):
                        go_to_detail(b['Block ID'], b['Block Name'])
                        st.rerun()
                    
                    if st.button("🗑️ Xóa", key=f"del_{b['Block ID']}", type="secondary"):
                        be.delete_block(st.secrets, b['Block ID'])
                        st.warning("Đã xóa!")
                        time.sleep(0.5)
                        st.rerun()

# ==============================================================================
# VIEW 2: CHI TIẾT KHỐI (DETAIL VIEW)
# ==============================================================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    # Header navigation
    c_back, c_title = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"):
        go_to_list()
        st.rerun()
    c_title.title(f"⚙️ Cấu hình: {b_name}")
    
    # 1. CÀI ĐẶT LỊCH CHẠY (SCHEDULE)
    with st.expander("⏰ Cài đặt Lịch chạy cho Khối này", expanded=True):
        freq = st.radio("Tần suất", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True, key="freq")
        
        sch_config = {}
        if freq == "Hàng ngày":
            t = st.time_input("Giờ chạy", dt_time(8,0))
            sch_config = {"time": str(t)}
        elif freq == "Hàng tuần":
            d = st.selectbox("Thứ", ["Thứ 2","Thứ 3","Thứ 4","Thứ 5","Thứ 6","Thứ 7","CN"])
            t = st.time_input("Giờ", dt_time(8,0))
            sch_config = {"day": d, "time": str(t)}
            
        if st.button("Lưu Lịch Chạy"):
            be.update_block_config(st.secrets, b_id, freq, sch_config)
            st.success("Đã lưu lịch!")

    st.divider()
    
    # 2. QUẢN LÝ DANH SÁCH LINK (Editable DataFrame)
    st.subheader("🔗 Danh sách Link API")
    
    # Load Links
    links = be.get_links_by_block(st.secrets, b_id)
    df_links = pd.DataFrame(links)
    
    if df_links.empty:
        st.info("Chưa có Link nào. Hãy thêm bên dưới.")
        # Tạo df rỗng có cấu trúc để hiển thị header
        df_links = pd.DataFrame(columns=["Link ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"])
    
    # Ẩn cột ID và Block ID khi hiển thị
    display_cols = ["Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"]
    
    # Convert Date columns
    if not df_links.empty:
        df_links["Date Start"] = pd.to_datetime(df_links["Date Start"], errors='coerce')
        df_links["Date End"] = pd.to_datetime(df_links["Date End"], errors='coerce')

    # [FIX] Đã bỏ type="password" vì Streamlit data_editor chưa hỗ trợ
    edited_links = st.data_editor(
        df_links,
        column_config={
            "Method": st.column_config.SelectboxColumn("Method", options=["GET", "POST"], width="small"),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Active", "Inactive"], width="small"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD/MM/YYYY"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD/MM/YYYY"),
            "Access Token": st.column_config.TextColumn("Token (Nhập lại nếu trống)"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link")
        },
        use_container_width=True,
        num_rows="dynamic", # Cho phép thêm dòng trực tiếp
        key="link_editor",
        hide_index=True
    )
    
    if st.button("💾 LƯU DANH SÁCH LINK"):
        # Save logic
        be.save_links_bulk(st.secrets, b_id, edited_links)
        st.success("Đã lưu danh sách link!")
        time.sleep(1)
        st.rerun()
