import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- SESSION STATE QUẢN LÝ ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""

# [FIX] Biến quản lý trạng thái tải dữ liệu
if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    # Reset trạng thái để tải dữ liệu mới của block này
    st.session_state['data_loaded'] = False 
    st.session_state['current_df'] = None

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

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

# --- LIST VIEW ---
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
    if blocks:
        df_b = pd.DataFrame(blocks).drop_duplicates(subset=["Block ID"])
        blocks = df_b.to_dict('records')
    
    if not blocks: st.info("Chưa có dữ liệu.")
    else:
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
                        
                        if not unique_links: st.warning("Không có Link nào 'Chưa chốt' để chạy.")
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

# --- DETAIL VIEW ---
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    with st.expander("⏰ Cài đặt Lịch chạy", expanded=False):
        freq = st.radio("Tần suất", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        sch_config = {}
        if freq == "Hàng ngày":
            t = st.time_input("Giờ", dt_time(8,0))
            sch_config = {"time": str(t)}
        elif freq == "Hàng tuần":
            d = st.selectbox("Thứ", ["Thứ 2","Thứ 3","Thứ 4","Thứ 5","Thứ 6","Thứ 7","CN"])
            t = st.time_input("Giờ", dt_time(8,0))
            sch_config = {"day": d, "time": str(t)}
        elif freq == "Hàng tháng":
            d = st.number_input("Ngày", 1, 31, 1)
            t = st.time_input("Giờ", dt_time(8,0))
            sch_config = {"day": d, "time": str(t)}
        if st.button("Lưu Lịch"):
            be.update_block_config(st.secrets, b_id, freq, sch_config)
            st.success("Đã lưu!")

    st.divider()
    st.subheader("🔗 Danh sách Link API")

    # --- [FIX QUAN TRỌNG] CHỈ LOAD DỮ LIỆU TỪ DB 1 LẦN ---
    if not st.session_state['data_loaded']:
        original_links = be.get_links_by_block(st.secrets, b_id)
        if original_links:
            df_temp = pd.DataFrame(original_links).drop_duplicates(subset=["Link ID"])
        else:
            df_temp = pd.DataFrame(columns=["Link ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"])
        
        # Lưu map token thật
        token_map = {}
        if not df_temp.empty:
            for _, row in df_temp.iterrows():
                token_map[str(row.get('Link ID', ''))] = row.get('Access Token', '')
        st.session_state['original_token_map'] = token_map

        # Xử lý hiển thị
        df_display = df_temp.copy()
        TOKEN_PLACEHOLDER = "✅ Đã lưu vào kho"
        df_display["Access Token"] = df_display["Access Token"].apply(lambda x: TOKEN_PLACEHOLDER if x and str(x).strip() else "")
        df_display["Date Start"] = pd.to_datetime(df_display["Date Start"], errors='coerce')
        df_display["Date End"] = pd.to_datetime(df_display["Date End"], errors='coerce')
        
        if "Method" in df_display.columns: df_display = df_display.drop(columns=["Method"])
        
        st.session_state['current_df'] = df_display
        st.session_state['data_loaded'] = True
    
    # --- HIỂN THỊ EDITOR ---
    # Luôn dùng dữ liệu từ session_state (đã bao gồm các dòng mới thêm chưa lưu)
    edited_df = st.data_editor(
        st.session_state['current_df'],
        column_config={
            "Link ID": st.column_config.TextColumn("ID (Auto)", disabled=True),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], width="medium", required=True),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD/MM/YYYY"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD/MM/YYYY"),
            "Access Token": st.column_config.TextColumn("Token (Bảo mật)", help="Xóa chữ 'Đã lưu' để nhập mới"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link")
        },
        use_container_width=True,
        num_rows="dynamic",
        key="link_editor",
        hide_index=True
    )
    
    # --- SAVE LOGIC ---
    if st.button("💾 LƯU DANH SÁCH LINK", type="primary"):
        try:
            real_map = st.session_state['original_token_map']
            TOKEN_PLACEHOLDER = "✅ Đã lưu vào kho"
            
            restored_rows = []
            for index, row in edited_df.iterrows():
                row_data = row.to_dict()
                l_id = str(row_data.get('Link ID', ''))
                current_display = str(row_data.get('Access Token', '')).strip()
                
                # Khôi phục token
                if current_display == TOKEN_PLACEHOLDER:
                    row_data['Access Token'] = real_map.get(l_id, "")
                else:
                    row_data['Access Token'] = current_display # Token mới
                
                row_data['Method'] = "GET"
                restored_rows.append(row_data)
            
            final_df = pd.DataFrame(restored_rows)
            
            # Lưu vào DB (Backend sẽ tự sinh ID 1->N)
            be.save_links_bulk(st.secrets, b_id, final_df)
            
            st.success("✅ Đã lưu cấu hình!")
            # Reset trạng thái để lần sau load lại dữ liệu mới từ DB (có ID 1,2,3...)
            st.session_state['data_loaded'] = False 
            st.session_state['current_df'] = None
            time.sleep(1)
            st.rerun()
            
        except Exception as e:
            st.error(f"Lỗi khi lưu: {str(e)}")
