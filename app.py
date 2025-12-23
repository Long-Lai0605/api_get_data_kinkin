import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- CẤU HÌNH TRANG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- KHỞI TẠO SESSION STATE ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""

if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

# --- KẾT NỐI DATABASE ---
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- ĐIỀU HƯỚNG ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    st.session_state['data_loaded'] = False 
    st.session_state['current_df'] = None

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# --- XỬ LÝ LOGIC CHẠY (REALTIME) ---
def run_link_process(link_data, block_name, status_container):
    url = link_data.get('API URL')
    token = link_data.get('Access Token')
    f_key = link_data.get('Filter Key')
    sheet_name = link_data.get('Sheet Name')
    link_sheet = link_data.get('Link Sheet')
    link_id = link_data.get('Link ID')
    
    # Xử lý ngày tháng an toàn
    d_s_raw = str(link_data.get('Date Start', '')).strip()
    d_e_raw = str(link_data.get('Date End', '')).strip()
    
    d_s = None
    if d_s_raw and d_s_raw.lower() not in ['none', 'nan', 'nat', '']:
        try:
            ts = pd.to_datetime(d_s_raw, dayfirst=True, errors='coerce')
            if not pd.isna(ts): d_s = ts.date()
        except: d_s = None

    d_e = None
    if d_e_raw and d_e_raw.lower() not in ['none', 'nan', 'nat', '']:
        try:
            ts = pd.to_datetime(d_e_raw, dayfirst=True, errors='coerce')
            if not pd.isna(ts): d_e = ts.date()
        except: d_e = None
    
    def cb(msg): status_container.write(f"👉 {msg}")
    
    data, msg = be.fetch_1office_data_smart(url, token, 'GET', f_key, d_s, d_e, cb)
    
    if msg == "Success" and data:
        status_container.write(f"✅ Tải {len(data)} dòng. Ghi Sheet...")
        
        # Hàm write_to_sheet_range trả về tổng số dòng đã ghi
        total_rows_str, w_msg = be.write_to_sheet_range(st.secrets, link_sheet, sheet_name, block_name, data)
        
        if "Error" not in w_msg:
            range_display = f"2 - {total_rows_str}"
            
            # 1. Backend: Ghi thẳng vào Sheet (Surgical Update)
            be.update_link_last_range(st.secrets, link_id, range_display)
            
            # 2. Frontend: Cập nhật ngay vào Session State để hiển thị
            if st.session_state['current_df'] is not None:
                try:
                    mask = st.session_state['current_df']['Link ID'].astype(str) == str(link_id)
                    if mask.any():
                        idx = st.session_state['current_df'].index[mask][0]
                        st.session_state['current_df'].at[idx, 'Last Range'] = range_display
                except: pass

            be.log_execution_history(st.secrets, f"{block_name} - {sheet_name}", "Manual", "Success", f"Updated {len(data)} rows")
            
            time.sleep(1) # Tránh Rate Limit
            return True, f"Xong! Dữ liệu: {range_display}"
        else:
            be.log_execution_history(st.secrets, f"{block_name} - {sheet_name}", "Manual", "Failed", f"Write Error: {w_msg}")
            return False, f"Lỗi ghi: {w_msg}"
            
    be.log_execution_history(st.secrets, f"{block_name} - {sheet_name}", "Manual", "Failed", f"Fetch Error: {msg}")
    return False, msg

# --- GIAO DIỆN: DANH SÁCH BLOCK ---
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
                        
                        if not unique_links: st.warning("Không có Link 'Chưa chốt' nào.")
                        else:
                            with st.status(f"Đang chạy {len(unique_links)} link...", expanded=True):
                                for l in unique_links:
                                    st.write(f"**--- {l.get('Sheet Name')} ---**")
                                    run_link_process(l, b['Block Name'], st)
                    else: st.warning("Khối trống!")
                
                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"):
                        go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"):
                        be.delete_block(st.secrets, b['Block ID']); st.rerun()

# --- GIAO DIỆN: CHI TIẾT BLOCK ---
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
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
    
    # --- CHECK PERMISSION ---
    c_h1, c_h2 = st.columns([3, 1])
    c_h1.subheader("🔗 Danh sách Link API")
    if c_h2.button("🛡️ Kiểm tra Quyền Ghi", type="secondary"):
        links_to_check = be.get_links_by_block(st.secrets, b_id)
        if not links_to_check: st.warning("Chưa có link (Hãy bấm Lưu trước).")
        else:
            unique_sheets = list(set([l.get("Link Sheet") for l in links_to_check if l.get("Link Sheet")]))
            with st.status("Đang kiểm tra...", expanded=True) as status:
                all_ok = True
                for url in unique_sheets:
                    ok, msg, bot_mail = be.check_sheet_access(st.secrets, url)
                    if ok: st.write(f"✅ {msg}: ...{url[-15:]}")
                    else:
                        all_ok = False; st.error(f"**{msg}**: ...{url[-15:]}")
                        st.code(bot_mail, language="text")
                        st.caption("Hãy thêm email trên vào nút Share (Quyền Editor).")
                if all_ok: status.update(label="✅ Tất cả OK!", state="complete", expanded=False)
                else: status.update(label="⚠️ Có Sheet lỗi quyền!", state="error")

    # --- DATA EDITOR ---
    if not st.session_state['data_loaded']:
        original_links = be.get_links_by_block(st.secrets, b_id)
        
        header_cols = ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"]
        
        if original_links:
            df_temp = pd.DataFrame(original_links).drop_duplicates(subset=["Link ID"])
        else:
            df_temp = pd.DataFrame(columns=header_cols)
        
        if "Last Range" not in df_temp.columns: df_temp["Last Range"] = ""
        if "Block ID" not in df_temp.columns: df_temp["Block ID"] = b_id

        token_map = {}
        if not df_temp.empty:
            for _, row in df_temp.iterrows():
                token_map[str(row.get('Link ID', ''))] = row.get('Access Token', '')
        st.session_state['original_token_map'] = token_map

        df_display = df_temp.copy()
        
        TOKEN_PLACEHOLDER = "✅ Đã lưu vào kho"
        df_display["Access Token"] = df_display["Access Token"].apply(lambda x: TOKEN_PLACEHOLDER if x and str(x).strip() else "")
        df_display["Date Start"] = pd.to_datetime(df_display["Date Start"], errors='coerce')
        df_display["Date End"] = pd.to_datetime(df_display["Date End"], errors='coerce')
        
        if "Method" in df_display.columns: df_display = df_display.drop(columns=["Method"])
        
        st.session_state['current_df'] = df_display
        st.session_state['data_loaded'] = True
    
    # CẤU HÌNH CỘT
    column_ordering = [
        "Link ID", "Block ID", "API URL", "Access Token", "Link Sheet", "Sheet Name", 
        "Filter Key", "Date Start", "Date End", "Last Range", "Status"
    ]

    edited_df = st.data_editor(
        st.session_state['current_df'],
        column_order=column_ordering,
        column_config={
            "Link ID": st.column_config.TextColumn("ID (Auto)", disabled=True, width="small"),
            "Block ID": st.column_config.TextColumn("ID Block", disabled=True, width="small"),
            "API URL": st.column_config.TextColumn("API URL", width="medium"),
            "Access Token": st.column_config.TextColumn("Token (Bảo mật)", help="Xóa chữ 'Đã lưu' để nhập mới", width="small"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link", width="medium"),
            "Sheet Name": st.column_config.TextColumn("Tên Sheet", width="small"),
            "Filter Key": st.column_config.TextColumn("Filter Key", width="small"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD-MM-YYYY", width="medium"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD-MM-YYYY", width="medium"),
            "Last Range": st.column_config.TextColumn("Dòng dữ liệu cập nhật", disabled=True, width="medium"),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], width="medium", required=True),
        },
        use_container_width=True,
        num_rows="dynamic",
        key="link_editor",
        hide_index=True
    )
    
    # --- CÁC NÚT BẤM ---
    col_act1, col_act2 = st.columns([1, 4])
    
    # Nút 1: Lưu Cấu Hình (Chỉ dùng khi sửa tay, KHÔNG dùng khi chạy)
    if col_act1.button("💾 LƯU DANH SÁCH", type="primary"):
        try:
            real_map = st.session_state['original_token_map']
            TOKEN_PLACEHOLDER = "✅ Đã lưu vào kho"
            
            restored_rows = []
            for index, row in edited_df.iterrows():
                row_data = row.to_dict()
                l_id = str(row_data.get('Link ID', ''))
                current_display = str(row_data.get('Access Token', '')).strip()
                
                if current_display == TOKEN_PLACEHOLDER:
                    row_data['Access Token'] = real_map.get(l_id, "")
                else:
                    row_data['Access Token'] = current_display 
                
                row_data['Method'] = "GET"
                if 'Block ID' not in row_data or not row_data['Block ID']:
                    row_data['Block ID'] = b_id
                restored_rows.append(row_data)
            
            final_df = pd.DataFrame(restored_rows)
            # Lưu đè toàn bộ (Bulk Save) - Chỉ an toàn khi đang ở trạng thái nghỉ
            be.save_links_bulk(st.secrets, b_id, final_df)
            
            st.success("✅ Đã lưu cấu hình!")
            st.session_state['data_loaded'] = False 
            st.session_state['current_df'] = None
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Lỗi khi lưu: {str(e)}")

    # Nút 2: CHẠY TẤT CẢ (ĐÃ SỬA: KHÔNG TỰ ĐỘNG LƯU ĐÈ TOÀN BỘ)
    if col_act2.button("🚀 CHẠY TẤT CẢ LINK", type="secondary"):
        rows_to_run = []
        for index, row in edited_df.iterrows():
            if row.get("Status") == "Chưa chốt & đang cập nhật":
                # Phục hồi token
                l_id = str(row.get('Link ID', ''))
                current_display = str(row.get('Access Token', '')).strip()
                real_token = st.session_state['original_token_map'].get(l_id, "")
                
                link_data = row.to_dict()
                if current_display == "✅ Đã lưu vào kho":
                    link_data['Access Token'] = real_token
                else:
                    link_data['Access Token'] = current_display
                
                rows_to_run.append(link_data)

        if not rows_to_run:
            st.warning("Không có link nào 'Chưa chốt' để chạy.")
        else:
            progress_text = "Đang xử lý..."
            my_bar = st.progress(0, text=progress_text)
            total = len(rows_to_run)
            
            for i, l in enumerate(rows_to_run):
                pct = int(((i) / total) * 100)
                my_bar.progress(pct, text=f"Đang chạy: {l.get('Sheet Name')} ({i+1}/{total})")
                
                # Hàm này đã: 
                # 1. Ghi vào Sheet (Backend)
                # 2. Cập nhật vào Session State (Memory)
                run_link_process(l, b_name, st)
                
                time.sleep(1)
            
            my_bar.progress(100, text="Hoàn thành!")
            st.success("✅ Đã chạy xong tất cả!")
            
            # QUAN TRỌNG: CHỈ RERUN ĐỂ HIỂN THỊ KẾT QUẢ TỪ BỘ NHỚ
            # KHÔNG XÓA SESSION STATE -> Giúp hiển thị ngay lập tức
            # KHÔNG GỌI save_links_bulk -> Tránh ghi đè dữ liệu cũ
            time.sleep(1)
            st.rerun()
