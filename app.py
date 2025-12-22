import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- QUẢN LÝ STATE ---
# Biến điều hướng
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""

# Biến dữ liệu (Quan trọng để không mất khi reload)
if 'detail_df' not in st.session_state: st.session_state['detail_df'] = None
if 'detail_token_map' not in st.session_state: st.session_state['detail_token_map'] = {}
if 'loaded_block_id' not in st.session_state: st.session_state['loaded_block_id'] = None

# Kết nối DB
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- NAVIGATION ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    # Khi chuyển block khác thì mới cần reset dữ liệu
    if st.session_state['loaded_block_id'] != b_id:
        st.session_state['detail_df'] = None
        st.session_state['detail_token_map'] = {}
        st.session_state['loaded_block_id'] = None

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

def force_reload_data():
    """Bắt buộc tải lại dữ liệu từ Sheet"""
    st.session_state['detail_df'] = None
    st.session_state['loaded_block_id'] = None

# --- RUN LOGIC ---
def run_link_process(link_data, block_name, status_container):
    url = link_data.get('API URL')
    token = link_data.get('Access Token')
    f_key = link_data.get('Filter Key')
    sheet_name = link_data.get('Sheet Name')
    
    d_s_raw = link_data.get('Date Start')
    d_e_raw = link_data.get('Date End')
    d_s = pd.to_datetime(d_s_raw).date() if d_s_raw else None
    d_e = pd.to_datetime(d_e_raw).date() if d_e_raw else None
    
    def cb(msg): status_container.write(f"👉 {msg}")
    
    data, msg = be.fetch_1office_data_smart(url, token, 'GET', f_key, d_s, d_e, cb)
    
    if msg == "Success" and data:
        status_container.write(f"✅ Tải {len(data)} dòng. Ghi Sheet...")
        res, w_msg = be.write_to_sheet_range(st.secrets, link_data.get('Link Sheet'), sheet_name, block_name, data)
        
        if "Error" not in w_msg:
            be.log_execution_history(st.secrets, f"{block_name} - {sheet_name}", "Manual", "Success", f"Updated {len(data)} rows")
            return True, f"Xong! {res}"
        else:
            be.log_execution_history(st.secrets, f"{block_name} - {sheet_name}", "Manual", "Failed", f"Write Error: {w_msg}")
            return False, f"Lỗi ghi: {w_msg}"
            
    be.log_execution_history(st.secrets, f"{block_name} - {sheet_name}", "Manual", "Failed", f"Fetch Error: {msg}")
    return False, msg

# ==============================================================================
# VIEW 1: DANH SÁCH KHỐI
# ==============================================================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    
    c1, c2 = st.columns([3, 1])
    c1.caption("Hệ thống quản lý Bot lấy dữ liệu tự động.")
    with c2:
        with st.popover("➕ Thêm Khối Mới", use_container_width=True):
            new_name = st.text_input("Tên Khối")
            if st.button("Tạo ngay") and new_name:
                be.create_block(st.secrets, new_name)
                st.success("Đã tạo!")
                time.sleep(0.5); st.rerun()

    blocks = be.get_all_blocks(st.secrets)
    if blocks:
        # Lọc trùng hiển thị
        df_b = pd.DataFrame(blocks).drop_duplicates(subset=["Block ID"])
        blocks = df_b.to_dict('records')
    
    if not blocks: st.info("Chưa có khối dữ liệu nào.")
    else:
        # Nút tiện ích
        if st.button("🔄 Làm mới danh sách", type="secondary"):
             st.cache_data.clear()
             st.rerun()

        if st.button("▶️ CHẠY TẤT CẢ (ALL BLOCKS)", type="primary"):
            st.toast("Khởi động chạy toàn bộ...")
            for b in blocks:
                st.write(f"🚀 **{b['Block Name']}**")
                links = be.get_links_by_block(st.secrets, b['Block ID'])
                seen = set(); valid_links = []
                for l in links:
                    if l.get("Link ID") not in seen and l.get("Status") == "Chưa chốt & đang cập nhật":
                        valid_links.append(l); seen.add(l.get("Link ID"))
                for l in valid_links:
                    with st.status(f"Run: {l.get('Sheet Name')}") as s:
                        run_link_process(l, b['Block Name'], s)
        st.divider()
        
        # Danh sách Block
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                col2.caption(f"Lịch: {b['Schedule Type']}")
                
                if col3.button("▶️ Chạy Khối", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    if links:
                        valid_links = [l for l in links if l.get("Status") == "Chưa chốt & đang cập nhật"]
                        seen = set(); unique_links = []
                        for l in valid_links:
                            if l['Link ID'] not in seen: unique_links.append(l); seen.add(l['Link ID'])
                        
                        if not unique_links: st.warning("Không có Link 'Chưa chốt' nào.")
                        else:
                            with st.status(f"Đang chạy {len(unique_links)} link...", expanded=True):
                                for l in unique_links:
                                    st.write(f"**--- {l.get('Sheet Name')} ---**")
                                    ok, msg = run_link_process(l, b['Block Name'], st)
                                    if ok: st.success(msg)
                                    else: st.error(msg)
                    else: st.warning("Khối trống!")
                
                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"):
                        go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"):
                        be.delete_block(st.secrets, b['Block ID']); st.rerun()

# ==============================================================================
# VIEW 2: CHI TIẾT KHỐI
# ==============================================================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    # 1. Header
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    # 2. Cài đặt Lịch
    with st.expander("⏰ Cài đặt Lịch chạy (Nâng cao)", expanded=True):
        freq = st.radio("Chọn Tần suất chính", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        sch_config = {}
        
        if freq == "Hàng ngày":
            st.write("---")
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                en_fixed = st.checkbox("Kích hoạt: Cố định 1 lần/ngày", value=False)
                t_fixed = st.time_input("Chọn giờ chạy (Cố định)", dt_time(8,0), disabled=not en_fixed)
            with col_d2:
                en_loop = st.checkbox("Kích hoạt: Lấy liên tục (Loop)", value=False)
                t_loop = st.number_input("Chạy lại sau mỗi (phút)", min_value=5, value=60, disabled=not en_loop)
            if en_fixed: sch_config["fixed_time"] = str(t_fixed)
            if en_loop: sch_config["loop_minutes"] = t_loop
            
        elif freq == "Hàng tuần":
            st.write("---")
            col_w1, col_w2 = st.columns(2)
            weekdays = ["Thứ 2","Thứ 3","Thứ 4","Thứ 5","Thứ 6","Thứ 7","CN"]
            with col_w1:
                st.markdown("##### 🗓️ Lần 1 (Bắt buộc)")
                d1 = st.selectbox("Thứ", weekdays, key="wd1")
                t1 = st.time_input("Giờ", dt_time(8,0), key="wt1")
                sch_config["run_1"] = {"day": d1, "time": str(t1)}
            with col_w2:
                en_w2 = st.checkbox("Kích hoạt: Lần 2")
                d2 = st.selectbox("Thứ", weekdays, key="wd2", disabled=not en_w2)
                t2 = st.time_input("Giờ", dt_time(17,0), key="wt2", disabled=not en_w2)
                if en_w2: sch_config["run_2"] = {"day": d2, "time": str(t2)}

        elif freq == "Hàng tháng":
            st.write("---")
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.markdown("##### 🗓️ Lần 1 (Bắt buộc)")
                d1 = st.number_input("Ngày (1-31)", 1, 31, 1, key="md1")
                t1 = st.time_input("Giờ", dt_time(8,0), key="mt1")
                sch_config["run_1"] = {"day": d1, "time": str(t1)}
            with col_m2:
                en_m2 = st.checkbox("Kích hoạt: Lần 2")
                d2 = st.number_input("Ngày (1-31)", 1, 31, 15, key="md2", disabled=not en_m2)
                t2 = st.time_input("Giờ", dt_time(17,0), key="mt2", disabled=not en_m2)
                if en_m2: sch_config["run_2"] = {"day": d2, "time": str(t2)}

        if st.button("💾 Lưu Cấu Hình Lịch Chạy", type="primary"):
            be.update_block_config_and_schedule(st.secrets, b_id, b_name, freq, sch_config)
            st.success("✅ Đã lưu cấu hình lịch chạy!")
            time.sleep(1)

    st.divider()
    
    # 3. Actions Toolbar
    c_h1, c_h2, c_h3 = st.columns([2, 1, 1])
    c_h1.subheader("🔗 Danh sách Link API")
    
    if c_h2.button("🔄 Tải lại gốc", help="Hủy bỏ thay đổi chưa lưu, lấy lại từ Sheet"):
        force_reload_data()
        st.toast("Đang tải lại dữ liệu từ Server...")
        time.sleep(0.5)
        st.rerun()

    if c_h3.button("🛡️ Kiểm tra Quyền", type="secondary"):
        # Lấy từ session_state (bản đang sửa) hoặc load mới
        current_links = []
        if st.session_state['detail_df'] is not None:
             current_links = st.session_state['detail_df'].to_dict('records')
        else:
             current_links = be.get_links_by_block(st.secrets, b_id)
             
        if not current_links: st.warning("Chưa có link.")
        else:
            unique_sheets = list(set([l.get("Link Sheet") for l in current_links if l.get("Link Sheet")]))
            with st.status("Đang kiểm tra...", expanded=True) as status:
                all_ok = True
                for url in unique_sheets:
                    ok, msg, bot_mail = be.check_sheet_access(st.
