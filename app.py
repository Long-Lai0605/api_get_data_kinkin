import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- CẤU HÌNH TRANG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- SESSION STATE ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""
if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

# --- KẾT NỐI ---
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- NAVIGATION ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    st.session_state['data_loaded'] = False 
    st.session_state['current_df'] = None

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# --- VIEW: LIST ---
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
        # NOTE: Nút chạy tất cả ở List View cũng nên áp dụng logic mới này nếu cần
        st.write("---")
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                col2.caption(f"Lịch: {b['Schedule Type']}")
                if col3.button("▶️ Chạy Khối", key=f"run_{b['Block ID']}"):
                    st.toast("Vui lòng vào chi tiết để chạy kiểm soát 4 trạng thái.")
                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"):
                        go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"):
                        be.delete_block(st.secrets, b['Block ID']); st.rerun()

# --- VIEW: DETAIL ---
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    # --- CONFIG SCHEDULE & PERMISSION (GIỮ NGUYÊN) ---
    with st.expander("⏰ Cài đặt Lịch chạy (Nâng cao)", expanded=True):
        freq = st.radio("Chọn Tần suất chính", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        sch_config = {} 
        # (Giữ nguyên logic config schedule...)
        if st.button("💾 Lưu Cấu Hình Lịch Chạy", type="primary"):
            be.update_block_config_and_schedule(st.secrets, b_id, b_name, freq, sch_config)
            st.success("✅ Đã lưu!")
            time.sleep(1)

    st.divider()
    
    # --- DATA EDITOR ---
    if not st.session_state['data_loaded']:
        original_links = be.get_links_by_block(st.secrets, b_id)
        header_cols = ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"]
        
        if original_links:
            df_temp = pd.DataFrame(original_links).drop_duplicates(subset=["Link ID"])
        else:
            df_temp = pd.DataFrame(columns=header_cols)
        
        if "Last Range" not in df_temp.columns: df_temp["Last Range"] = ""
        df_temp["Block ID"] = b_id

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
    
    column_ordering = [
        "Link ID", "Block ID", "API URL", "Access Token", "Link Sheet", "Sheet Name", 
        "Filter Key", "Date Start", "Date End", "Last Range", "Status"
    ]

    edited_df = st.data_editor(
        st.session_state['current_df'],
        column_order=column_ordering,
        column_config={
            "Link ID": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "Block ID": st.column_config.TextColumn("ID Block", disabled=True, width="small"),
            "API URL": st.column_config.TextColumn("API URL", width="medium"),
            "Access Token": st.column_config.TextColumn("Token", width="small"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link", width="medium"),
            "Sheet Name": st.column_config.TextColumn("Tên Sheet", width="small"),
            "Filter Key": st.column_config.TextColumn("Filter Key", width="small"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD-MM-YYYY", width="medium"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD-MM-YYYY", width="medium"),
            "Last Range": st.column_config.TextColumn("Dòng cập nhật", disabled=True, width="medium"),
            # OPTIONS CHO 4 TRẠNG THÁI
            "Status": st.column_config.SelectboxColumn(
                "Trạng thái", 
                options=[
                    "Chưa chốt & đang cập nhật", 
                    "Cập nhật dữ liệu cũ", 
                    "Cập nhật dữ liệu mới", 
                    "Đã chốt"
                ], 
                width="medium", 
                required=True
            ),
        },
        use_container_width=True,
        num_rows="dynamic",
        key="link_editor",
        hide_index=True
    )
    
    # --- ACTION BUTTONS ---
    col_act1, col_act2 = st.columns([1, 4])
    
    # 1. NÚT LƯU CONFIG
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
            be.save_links_bulk(st.secrets, b_id, final_df)
            st.success("✅ Đã lưu!")
            st.session_state['data_loaded'] = False 
            st.session_state['current_df'] = None
            time.sleep(1)
            st.rerun()
        except Exception as e: st.error(str(e))

    # 2. NÚT CHẠY 4 TRẠNG THÁI
    if col_act2.button("🚀 CHẠY THEO TRẠNG THÁI", type="secondary"):
        rows_to_run = []
        for index, row in edited_df.iterrows():
            status = row.get("Status")
            # Chỉ chạy nếu KHÔNG PHẢI Đã chốt
            if status != "Đã chốt":
                l_id = str(row.get('Link ID', ''))
                current_display = str(row.get('Access Token', '')).strip()
                real_token = st.session_state['original_token_map'].get(l_id, "")
                
                link_data = row.to_dict()
                if current_display == "✅ Đã lưu vào kho":
                    link_data['Access Token'] = real_token
                else:
                    link_data['Access Token'] = current_display
                
                link_data['Block ID'] = b_id
                rows_to_run.append(link_data)

        if not rows_to_run:
            st.warning("Không có link nào cần chạy (Tất cả đã chốt).")
        else:
            progress_text = "Đang xử lý..."
            my_bar = st.progress(0, text=progress_text)
            total = len(rows_to_run)
            
            for i, l in enumerate(rows_to_run):
                status_raw = l.get('Status')
                target_sheet = l.get('Sheet Name')
                pct = int(((i) / total) * 100)
                my_bar.progress(pct, text=f"Đang chạy: {target_sheet} [{status_raw}] ({i+1}/{total})")
                
                # Fetch Data
                d_s_raw = str(l.get('Date Start', '')).strip()
                d_e_raw = str(l.get('Date End', '')).strip()
                d_s, d_e = None, None
                try: 
                    if d_s_raw and d_s_raw.lower() not in ['none', 'nan', 'nat', '']: d_s = pd.to_datetime(d_s_raw, dayfirst=True).date()
                    if d_e_raw and d_e_raw.lower() not in ['none', 'nan', 'nat', '']: d_e = pd.to_datetime(d_e_raw, dayfirst=True).date()
                except: pass

                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], d_s, d_e, None)
                
                if msg == "Success":
                    # --- GỌI HÀM V4 XỬ LÝ 4 TRẠNG THÁI ---
                    count, w_msg = be.process_data_final_v4(
                        st.secrets, l['Link Sheet'], l['Sheet Name'], 
                        l['Block ID'], l['Link ID'], 
                        data, status_raw
                    )
                    
                    if "Error" not in w_msg:
                        # Update Dashboard Realtime
                        display_msg = f"OK: {count} dòng"
                        be.update_link_last_range(st.secrets, l['Link ID'], l['Block ID'], display_msg)
                        
                        # Update Local State
                        try:
                            mask = st.session_state['current_df']['Link ID'].astype(str) == str(l['Link ID'])
                            if mask.any():
                                idx = st.session_state['current_df'].index[mask][0]
                                st.session_state['current_df'].at[idx, 'Last Range'] = display_msg
                        except: pass
                    else:
                        st.error(f"Lỗi ghi {target_sheet}: {w_msg}")
                else:
                    st.error(f"Lỗi API {target_sheet}: {msg}")
                
                time.sleep(1)
            
            my_bar.progress(100, text="Hoàn thành!")
            st.success("✅ Đã xử lý xong!")
            time.sleep(1)
            st.rerun()
