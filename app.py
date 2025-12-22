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
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""

with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- NAVIGATION ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# --- RUN LOGIC ---
def run_link_process(link_data, block_name, status_container):
    url = link_data.get('API URL')
    token = link_data.get('Access Token')
    f_key = link_data.get('Filter Key')
    
    d_s_raw = link_data.get('Date Start')
    d_e_raw = link_data.get('Date End')
    d_s = pd.to_datetime(d_s_raw).date() if d_s_raw else None
    d_e = pd.to_datetime(d_e_raw).date() if d_e_raw else None
    
    def cb(msg): status_container.write(f"👉 {msg}")
    
    data, msg = be.fetch_1office_data_smart(url, token, 'GET', f_key, d_s, d_e, cb)
    
    if msg == "Success" and data:
        status_container.write(f"✅ Tải {len(data)} dòng. Ghi Sheet...")
        res, w_msg = be.write_to_sheet_range(st.secrets, link_data.get('Link Sheet'), link_data.get('Sheet Name'), block_name, data)
        if "Error" not in w_msg: return True, f"Xong! {res}"
        else: return False, f"Lỗi ghi: {w_msg}"
    return False, msg

# ==============================================================================
# VIEW 1: DANH SÁCH KHỐI
# ==============================================================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    
    c1, c2 = st.columns([3, 1])
    c1.caption("Quản lý các khối dữ liệu và lịch chạy.")
    with c2:
        with st.popover("➕ Thêm Khối Mới", use_container_width=True):
            new_name = st.text_input("Tên Khối")
            if st.button("Tạo ngay") and new_name:
                be.create_block(st.secrets, new_name)
                st.success("Đã tạo!")
                time.sleep(0.5); st.rerun()

    blocks = be.get_all_blocks(st.secrets)
    
    if not blocks: st.info("Chưa có dữ liệu.")
    else:
        if st.button("▶️ CHẠY TẤT CẢ (ALL BLOCKS)", type="primary"):
            st.toast("Khởi động chạy toàn bộ...")
            for b in blocks:
                st.write(f"🚀 **{b['Block Name']}**")
                links = be.get_links_by_block(st.secrets, b['Block ID'])
                for l in links:
                    if l.get("Status") == "Active":
                        with st.status(f"Run: {l.get('Sheet Name')}") as s:
                            run_link_process(l, b['Block Name'], s)
        st.divider()
        
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                col2.caption(f"Lịch: {b['Schedule Type']}")
                
                if col3.button("▶️ Chạy Khối", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    if links:
                        with st.status(f"Đang chạy {b['Block Name']}...", expanded=True):
                            for l in links:
                                if l.get("Status") == "Active":
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
# VIEW 2: CHI TIẾT KHỐI (QUẢN LÝ LINK + TOKEN MASKING)
# ==============================================================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    # 1. SCHEDULE
    with st.expander("⏰ Cài đặt Lịch chạy", expanded=False):
        freq = st.radio("Tần suất", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        sch_config = {}
        if freq == "Hàng ngày":
            t = st.time_input("Giờ", dt_time(8,0))
            sch_config = {"time": str(t)}
        # ... (Giản lược code UI lịch cho gọn, logic backend đã có) ...
        if st.button("Lưu Lịch"):
            be.update_block_config(st.secrets, b_id, freq, sch_config)
            st.success("Đã lưu!")

    st.divider()
    st.subheader("🔗 Danh sách Link API")

    # --- LOGIC HIỂN THỊ TOKEN BẢO MẬT ---
    # 1. Lấy dữ liệu gốc từ DB
    original_links = be.get_links_by_block(st.secrets, b_id)
    df_original = pd.DataFrame(original_links)
    
    if df_original.empty:
        df_display = pd.DataFrame(columns=["Link ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"])
    else:
        # 2. Tạo bản sao để hiển thị (Masking Token)
        df_display = df_original.copy()
        
        # MASKING: Nếu có token -> Thay bằng "Đã lưu vào kho", ngược lại để trống
        # Dùng lambda kiểm tra độ dài để chắc chắn token hợp lệ
        TOKEN_PLACEHOLDER = "✅ Đã lưu vào kho"
        df_display["Access Token"] = df_display["Access Token"].apply(
            lambda x: TOKEN_PLACEHOLDER if x and len(str(x).strip()) > 0 else ""
        )
        
        # Convert Date for editor
        df_display["Date Start"] = pd.to_datetime(df_display["Date Start"], errors='coerce')
        df_display["Date End"] = pd.to_datetime(df_display["Date End"], errors='coerce')

    # 3. Hiển thị bảng Edit
    edited_df = st.data_editor(
        df_display,
        column_config={
            "Link ID": st.column_config.TextColumn("ID", disabled=True), # Khóa ID
            "Method": st.column_config.SelectboxColumn("Method", options=["GET", "POST"], width="small"),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Active", "Inactive"], width="small"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD/MM/YYYY"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD/MM/YYYY"),
            # Cột Token: Để TextColumn bình thường để user có thể xóa đi paste cái mới
            "Access Token": st.column_config.TextColumn("Token (Bảo mật)", help="Xóa chữ 'Đã lưu' để nhập token mới"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link")
        },
        use_container_width=True,
        num_rows="dynamic",
        key="link_editor",
        hide_index=True
    )
    
    # 4. Logic LƯU: Khôi phục Token thật
    if st.button("💾 LƯU DANH SÁCH LINK", type="primary"):
        try:
            # Tạo list chứa dữ liệu chuẩn để lưu
            final_links_to_save = pd.DataFrame()
            
            # Map dữ liệu gốc để tra cứu nhanh: {LinkID: RealToken}
            real_token_map = {}
            if not df_original.empty:
                real_token_map = dict(zip(df_original['Link ID'].astype(str), df_original['Access Token']))
            
            # Duyệt qua từng dòng đã sửa trên giao diện
            restored_rows = []
            for index, row in edited_df.iterrows():
                row_data = row.to_dict()
                l_id = str(row_data.get('Link ID', ''))
                current_token_display = str(row_data.get('Access Token', '')).strip()
                
                # LOGIC KHÔI PHỤC QUAN TRỌNG:
                # Nếu User giữ nguyên "Đã lưu vào kho" -> Lấy lại token gốc từ map
                if current_token_display == TOKEN_PLACEHOLDER:
                    # Lấy token gốc, nếu không tìm thấy (trường hợp lỗi lạ) thì để rỗng
                    real_token = real_token_map.get(l_id, "")
                    row_data['Access Token'] = real_token
                else:
                    # Nếu User đã sửa thành cái khác (hoặc dòng mới) -> Lấy giá trị user nhập
                    row_data['Access Token'] = current_token_display
                
                restored_rows.append(row_data)
            
            # Chuyển về DataFrame
            final_df = pd.DataFrame(restored_rows)
            
            # Gọi hàm lưu backend
            be.save_links_bulk(st.secrets, b_id, final_df)
            st.success("✅ Đã lưu cấu hình & Cập nhật Token bảo mật!")
            time.sleep(1)
            st.rerun()
            
        except Exception as e:
            st.error(f"Lỗi khi lưu: {str(e)}")
