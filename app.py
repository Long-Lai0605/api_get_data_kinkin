import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- CẤU HÌNH TRANG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- STATE ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""
# Biến kiểm soát việc load data (chỉ load 1 lần đầu hoặc khi force reload)
if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

# --- KẾT NỐI ---
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- HELPER ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    st.session_state['data_loaded'] = False # Reset để load lại data mới của block này
    st.session_state['current_df'] = None

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# --- VIEW LIST ---
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
        st.write("---")
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                col2.caption(f"Lịch: {b['Schedule Type']}")
                
                # Nút Chạy Khối (Logic: Fetch DB -> Run -> Update DB Result)
                # Ở màn hình List, ta không sửa cấu hình nên cứ lấy từ DB chạy
                if col3.button("▶️ Chạy Khối", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    if not links: st.warning("Chưa có Link nào.")
                    else:
                        with st.status(f"Đang chạy khối {b['Block Name']}...", expanded=True):
                            for l in links:
                                status_raw = l.get('Status')
                                if status_raw == "Đã chốt": continue
                                st.write(f"🔄 {l.get('Sheet Name')}")
                                
                                d_s_raw = str(l.get('Date Start', '')).strip()
                                d_e_raw = str(l.get('Date End', '')).strip()
                                d_s, d_e = None, None
                                try:
                                    if d_s_raw and d_s_raw.lower() not in ['none','']: d_s = pd.to_datetime(d_s_raw, dayfirst=True).date()
                                    if d_e_raw and d_e_raw.lower() not in ['none','']: d_e = pd.to_datetime(d_e_raw, dayfirst=True).date()
                                except: pass

                                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], d_s, d_e, None)
                                if msg == "Success":
                                    range_str, w_msg = be.process_data_final_v9(st.secrets, l['Link Sheet'], l['Sheet Name'], l['Block ID'], l['Link ID'], data, status_raw)
                                    if "Error" not in w_msg:
                                        be.update_link_last_range(st.secrets, l['Link ID'], l['Block ID'], range_str)
                                        st.write(f"✅ Xong: {range_str}")
                                    else: st.error(f"Lỗi ghi: {w_msg}")
                                else: st.error(f"Lỗi API: {msg}")
                        st.success("Hoàn thành khối!")

                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"):
                        go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"):
                        be.delete_block(st.secrets, b['Block ID']); st.rerun()

# --- VIEW DETAIL ---
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    with st.expander("⏰ Cài đặt Lịch chạy", expanded=True):
        freq = st.radio("Tần suất", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        sch_config = {}
        if freq == "Hàng ngày":
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
            st.write("---") # (Giữ nguyên)
        
        if st.button("💾 Lưu Cấu Hình Lịch Chạy", type="primary"):
            be.update_block_config_and_schedule(st.secrets, b_id, b_name, freq, sch_config)
            st.success("✅ Đã lưu!")
            time.sleep(1)

    st.divider()
    
    # 1. LOAD DATA: Chỉ load 1 lần khi vào trang, sau đó dùng session_state
    if not st.session_state['data_loaded']:
        original_links = be.get_links_by_block(st.secrets, b_id)
        header_cols = ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"]
        
        if original_links: df_temp = pd.DataFrame(original_links).drop_duplicates(subset=["Link ID"])
        else: df_temp = pd.DataFrame(columns=header_cols)
        
        if "Last Range" not in df_temp.columns: df_temp["Last Range"] = ""
        df_temp["Block ID"] = b_id
        
        token_map = {}
        if not df_temp.empty:
            for _, row in df_temp.iterrows():
                token_map[str(row.get('Link ID', ''))] = row.get('Access Token', '')
        st.session_state['original_token_map'] = token_map
        
        df_display = df_temp.copy()
        df_display["Access Token"] = df_display["Access Token"].apply(lambda x: "✅ Đã lưu vào kho" if x and str(x).strip() else "")
        df_display["Date Start"] = pd.to_datetime(df_display["Date Start"], errors='coerce')
        df_display["Date End"] = pd.to_datetime(df_display["Date End"], errors='coerce')
        
        # Chỉ giữ lại các cột hiển thị
        display_cols = ["Link ID", "Block ID", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Last Range", "Status"]
        # Đảm bảo đủ cột
        for c in display_cols:
            if c not in df_display.columns: df_display[c] = ""
            
        st.session_state['current_df'] = df_display[display_cols]
        st.session_state['data_loaded'] = True
    
    # 2. DATA EDITOR: Chỉnh sửa trực tiếp trên Local State
    edited_df = st.data_editor(
        st.session_state['current_df'],
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
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Cập nhật dữ liệu cũ", "Cập nhật dữ liệu mới", "Đã chốt"], width="medium", required=True),
        },
        use_container_width=True,
        num_rows="dynamic",
        key="link_editor",
        hide_index=True
    )
    
    # Hàm chuẩn bị dữ liệu từ Editor để lưu
    def prepare_data_to_save(df_input, token_map, block_id):
        rows = []
        for idx, r in df_input.iterrows():
            d = r.to_dict()
            lid = str(d.get('Link ID', ''))
            # Phục hồi token
            if d.get('Access Token') == "✅ Đã lưu vào kho": 
                d['Access Token'] = token_map.get(lid, "")
            d['Method'] = "GET"
            if not d.get('Block ID'): d['Block ID'] = block_id
            rows.append(d)
        return rows

    c1, c2 = st.columns([1, 4])
    
    # 3. NÚT LƯU: Chỉ khi ấn mới lưu xuống DB
    if c1.button("💾 LƯU DANH SÁCH", type="primary"):
        try:
            data_to_save = prepare_data_to_save(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(data_to_save))
            
            # Cập nhật lại session state sau khi lưu thành công để đồng bộ
            st.session_state['current_df'] = edited_df
            st.success("✅ Đã lưu xuống Database!")
            time.sleep(1)
            st.rerun() # Refresh để đảm bảo nhất quán
        except Exception as e: st.error(str(e))

    # 4. NÚT CHẠY: Tự động Lưu -> Sau đó Chạy
    if c2.button("🚀 LƯU & CHẠY NGAY", type="secondary"):
        # BƯỚC 1: LƯU TỰ ĐỘNG
        try:
            data_to_run = prepare_data_to_save(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(data_to_run))
            st.toast("✅ Đã tự động lưu cấu hình!")
        except Exception as e:
            st.error(f"Lỗi khi lưu tự động: {e}")
            st.stop()

        # BƯỚC 2: CHẠY
        valid_rows = [r for r in data_to_run if r.get('Status') != "Đã chốt"]
        
        if not valid_rows:
            st.warning("Không có link nào cần chạy.")
        else:
            prog = st.progress(0, text="Đang xử lý...")
            tot = len(valid_rows)
            for i, l in enumerate(valid_rows):
                stt = l.get('Status')
                target_sheet = l.get('Sheet Name')
                prog.progress(int(((i)/tot)*100), text=f"Đang chạy: {target_sheet} [{stt}]")
                
                ds_raw = str(l.get('Date Start', '')).strip()
                de_raw = str(l.get('Date End', '')).strip()
                ds, de = None, None
                try: 
                    if ds_raw and ds_raw.lower() not in ['none','']: ds = pd.to_datetime(ds_raw, dayfirst=True).date()
                    if de_raw and de_raw.lower() not in ['none','']: de = pd.to_datetime(de_raw, dayfirst=True).date()
                except: pass

                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                
                if msg == "Success":
                    range_str, w_msg = be.process_data_final_v9(st.secrets, l['Link Sheet'], l['Sheet Name'], l['Block ID'], l['Link ID'], data, stt)
                    
                    if "Error" not in w_msg:
                        # Update DB
                        be.update_link_last_range(st.secrets, l['Link ID'], l['Block ID'], range_str)
                        # Update UI State ngay lập tức (không cần load lại từ DB)
                        try:
                            lid_t = str(l['Link ID']).strip()
                            mask = st.session_state['current_df']['Link ID'].astype(str).str.strip() == lid_t
                            if mask.any():
                                ix = st.session_state['current_df'].index[mask][0]
                                st.session_state['current_df'].at[ix, 'Last Range'] = range_str
                        except: pass
                    else: st.error(f"Lỗi ghi {target_sheet}: {w_msg}")
                else: st.error(f"Lỗi API {target_sheet}: {msg}")
                time.sleep(1)
            
            prog.progress(100, text="Hoàn thành!")
            st.success("✅ Đã xử lý xong!")
            time.sleep(1)
            st.rerun() # Reload để hiển thị kết quả Last Range mới nhất
