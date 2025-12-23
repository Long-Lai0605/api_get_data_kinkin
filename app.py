import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- CONFIG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- STATE ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""
if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

# --- INIT DB ---
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- CACHING FUNCTIONS (FIX LỖI MÀN HÌNH CHÍNH) ---
@st.cache_data(ttl=300) # Cache 5 phút
def get_cached_blocks():
    return be.get_all_blocks(st.secrets)

def clear_cache():
    st.cache_data.clear()

# --- NAV ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    st.session_state['data_loaded'] = False 
    st.session_state['current_df'] = None

def go_to_list():
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# ==========================================
# VIEW: LIST (DANH SÁCH)
# ==========================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    
    c1, c2, c3 = st.columns([6, 1, 1])
    c1.caption("Quản lý các khối dữ liệu và lịch chạy.")
    
    if c2.button("🔄 Refresh"): # Nút Refresh thủ công
        clear_cache()
        st.rerun()

    with c3:
        with st.popover("➕ Thêm Khối", use_container_width=True):
            new_name = st.text_input("Tên Khối")
            if st.button("Tạo ngay") and new_name:
                be.create_block(st.secrets, new_name)
                clear_cache() # Xóa cache để hiện khối mới
                st.success("Đã tạo!")
                time.sleep(0.5); st.rerun()

    # Lấy dữ liệu từ CACHE (Nhanh & Ổn định)
    blocks = get_cached_blocks()
    
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
                
                # CHẠY KHỐI
                if col3.button("▶️ Chạy Khối", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    if not links: st.warning("Chưa có Link nào.")
                    else:
                        with st.status(f"Đang chạy khối {b['Block Name']}...", expanded=True):
                            for l in links:
                                stt = l.get('Status')
                                if stt == "Đã chốt": continue
                                st.write(f"🔄 {l.get('Sheet Name')}")
                                
                                ds, de = None, None # Parse date logic here...
                                try:
                                    d_s_raw = str(l.get('Date Start', '')).strip()
                                    d_e_raw = str(l.get('Date End', '')).strip()
                                    if d_s_raw and d_s_raw.lower() not in ['none','']: ds = pd.to_datetime(d_s_raw, dayfirst=True).date()
                                    if d_e_raw and d_e_raw.lower() not in ['none','']: de = pd.to_datetime(d_e_raw, dayfirst=True).date()
                                except: pass

                                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                                if msg == "Success":
                                    range_str, w_msg = be.process_data_final_v11(
                                        st.secrets, l['Link Sheet'], l['Sheet Name'],
                                        l['Block ID'], l['Link ID'], data, stt
                                    )
                                    if "Error" not in w_msg:
                                        be.update_link_last_range(st.secrets, l['Link ID'], l['Block ID'], range_str)
                                        st.write(f"✅ Xong: {range_str}")
                                    else: st.error(f"Lỗi: {w_msg}")
                                else: st.error(f"Lỗi API: {msg}")
                        st.success("Hoàn thành!")

                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"):
                        go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"):
                        be.delete_block(st.secrets, b['Block ID'])
                        clear_cache() # Xóa cache sau khi xóa
                        st.rerun()

# ==========================================
# VIEW: DETAIL (CHI TIẾT)
# ==========================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    with st.expander("⏰ Cài đặt Lịch chạy", expanded=True):
        freq = st.radio("Tần suất", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        sch_config = {}
        # (Config logic giữ nguyên như cũ...)
        if st.button("💾 Lưu Cấu Hình Lịch", type="primary"):
            be.update_block_config_and_schedule(st.secrets, b_id, b_name, freq, sch_config)
            clear_cache() # Xóa cache để cập nhật list bên ngoài
            st.success("✅ Đã lưu!")
            time.sleep(1)

    st.divider()
    
    # 1. LOAD DATA VÀO LOCAL STATE
    if not st.session_state['data_loaded']:
        original_links = be.get_links_by_block(st.secrets, b_id)
        header_cols = ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"]
        
        if original_links: df_temp = pd.DataFrame(original_links).drop_duplicates(subset=["Link ID"])
        else: df_temp = pd.DataFrame(columns=header_cols)
        
        if "Last Range" not in df_temp.columns: df_temp["Last Range"] = ""
        df_temp["Block ID"] = b_id
        
        token_map = {}
        if not df_temp.empty:
            for _, row in df_temp.iterrows(): token_map[str(row.get('Link ID', ''))] = row.get('Access Token', '')
        st.session_state['original_token_map'] = token_map
        
        df_display = df_temp.copy()
        df_display["Access Token"] = df_display["Access Token"].apply(lambda x: "✅ Đã lưu vào kho" if x and str(x).strip() else "")
        df_display["Date Start"] = pd.to_datetime(df_display["Date Start"], errors='coerce')
        df_display["Date End"] = pd.to_datetime(df_display["Date End"], errors='coerce')
        
        cols = ["Link ID", "Block ID", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Last Range", "Status"]
        for c in cols: 
            if c not in df_display.columns: df_display[c] = ""
        st.session_state['current_df'] = df_display[cols]
        st.session_state['data_loaded'] = True
    
    # 2. EDITOR
    edited_df = st.data_editor(
        st.session_state['current_df'],
        column_config={
            "Link ID": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "Block ID": st.column_config.TextColumn("Block", disabled=True, width="small"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link", width="medium"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD-MM-YYYY", width="medium"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD-MM-YYYY", width="medium"),
            "Last Range": st.column_config.TextColumn("Range", disabled=True, width="medium"),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Cập nhật dữ liệu cũ", "Cập nhật dữ liệu mới", "Đã chốt"], width="medium", required=True),
        },
        use_container_width=True, num_rows="dynamic", key="link_editor", hide_index=True
    )
    
    # Helper prepare data
    def prep_data(df, t_map, bid):
        rows = []
        for _, r in df.iterrows():
            d = r.to_dict()
            lid = str(d.get('Link ID', ''))
            if d.get('Access Token') == "✅ Đã lưu vào kho": d['Access Token'] = t_map.get(lid, "")
            d['Method'] = "GET"
            if not d.get('Block ID'): d['Block ID'] = bid
            rows.append(d)
        return rows

    c1, c2 = st.columns([1, 4])
    
    # NÚT LƯU
    if c1.button("💾 LƯU DANH SÁCH", type="primary"):
        try:
            d = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d))
            st.session_state['current_df'] = edited_df
            st.success("✅ Đã lưu!"); time.sleep(1); st.rerun()
        except Exception as e: st.error(str(e))

    # NÚT CHẠY (AUTO SAVE)
    if c2.button("🚀 LƯU & CHẠY NGAY", type="secondary"):
        try:
            d_run = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d_run)) # Auto Save
            st.toast("✅ Đã lưu cấu hình!")
        except Exception as e: st.error(str(e)); st.stop()

        valid_rows = [r for r in d_run if r.get('Status') != "Đã chốt"]
        if not valid_rows: st.warning("Không có link nào cần chạy.")
        else:
            prog = st.progress(0, text="Đang xử lý...")
            tot = len(valid_rows)
            for i, l in enumerate(valid_rows):
                stt = l.get('Status')
                prog.progress(int(((i)/tot)*100), text=f"Đang chạy: {l.get('Sheet Name')}")
                
                ds, de = None, None
                try: 
                    d_s_raw = str(l.get('Date Start', '')).strip()
                    d_e_raw = str(l.get('Date End', '')).strip()
                    if d_s_raw and d_s_raw.lower() not in ['none','']: ds = pd.to_datetime(d_s_raw, dayfirst=True).date()
                    if d_e_raw and d_e_raw.lower() not in ['none','']: de = pd.to_datetime(d_e_raw, dayfirst=True).date()
                except: pass

                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                if msg == "Success":
                    range_str, w_msg = be.process_data_final_v11(st.secrets, l['Link Sheet'], l['Sheet Name'], l['Block ID'], l['Link ID'], data, stt)
                    if "Error" not in w_msg:
                        be.update_link_last_range(st.secrets, l['Link ID'], l['Block ID'], range_str)
                        try: # Update Local UI
                            lid = str(l['Link ID']).strip()
                            msk = st.session_state['current_df']['Link ID'].astype(str).str.strip() == lid
                            if msk.any():
                                ix = st.session_state['current_df'].index[msk][0]
                                st.session_state['current_df'].at[ix, 'Last Range'] = range_str
                        except: pass
                    else: st.error(f"Lỗi: {w_msg}")
                else: st.error(f"Lỗi API: {msg}")
                time.sleep(1)
            
            prog.progress(100, text="Hoàn thành!"); st.success("Xong!"); time.sleep(1); st.rerun()
