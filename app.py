import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- 2. HỆ THỐNG ĐĂNG NHẬP ---
CREDENTIALS = {
    "admin": "admin888",
    "kinkin": "kinkin2025",
    "user": "user123"
}

if 'authenticated' not in st.session_state: st.session_state['authenticated'] = False
if 'user_role' not in st.session_state: st.session_state['user_role'] = ""

def check_login():
    u = st.session_state['input_username']
    p = st.session_state['input_password']
    if u in CREDENTIALS and CREDENTIALS[u] == p:
        st.session_state['authenticated'] = True
        st.session_state['user_role'] = u
    else: st.error("❌ Sai tên đăng nhập hoặc mật khẩu!")

def logout():
    st.session_state['authenticated'] = False; st.session_state['view'] = 'list'; st.rerun()

if not st.session_state['authenticated']:
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.container(border=True):
            st.title("🔒 KINKIN LOGIN")
            st.text_input("Tên đăng nhập", key="input_username")
            st.text_input("Mật khẩu", type="password", key="input_password")
            st.button("Đăng Nhập", type="primary", on_click=check_login, use_container_width=True)
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.write(f"👤 **{st.session_state['user_role'].upper()}**")
    if st.button("Đăng xuất", type="secondary"): logout()

# --- 3. STATE & DB ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""
if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- CACHE ---
@st.cache_data(ttl=300)
def get_cached_blocks(): return be.get_all_blocks(st.secrets)
def clear_cache(): st.cache_data.clear()

# --- HELPER UI ---
def format_schedule_display(sch_type, sch_config_str):
    if sch_type == "Thủ công": return "Thủ công"
    try:
        cfg = json.loads(sch_config_str) if isinstance(sch_config_str, str) else sch_config_str
        if sch_type == "Hàng ngày": return f"📅 Hàng ngày | {cfg.get('fixed_time','')}"
        elif sch_type == "Hàng tuần": return "🗓️ Hàng tuần"
        elif sch_type == "Hàng tháng": return "📆 Hàng tháng"
    except: return sch_type
    return sch_type

# --- POPUP HƯỚNG DẪN ---
@st.dialog("📖 TÀI LIỆU HƯỚNG DẪN SỬ DỤNG", width="large")
def show_user_guide():
    st.markdown("""
    ## 1. TỔNG QUAN & CÁC CHẾ ĐỘ
    | Chế độ | Hành động | Khi nào dùng? |
    | :--- | :--- | :--- |
    | **1. Chưa chốt & đang cập nhật** | Xóa cũ - Thay mới | Dữ liệu tháng hiện tại. |
    | **2. Cập nhật dữ liệu cũ** | Chỉ sửa cái đã có | Dữ liệu đã chốt danh sách. |
    | **3. Cập nhật dữ liệu mới** | Chỉ thêm cái chưa có | Log lịch sử. |
    | **4. Đã chốt** | Không làm gì | Dữ liệu quá khứ an toàn. |

    ## 2. LƯU Ý TỐC ĐỘ
    * **< 1k dòng:** ~30s | **10k dòng:** ~3-5p | **> 50k dòng:** ~15-30p.
    * **Lời khuyên:** Chia nhỏ dữ liệu bằng bộ lọc để chạy nhanh hơn.

    ## 3. THAO TÁC
    1. **Tạo Khối:** Thêm khối mới.
    2. **Cấu hình:** Nhập API, Token, Sheet Link.
    3. **Bộ lọc:** Điền `Filter Key` + Ngày tháng để chạy nhanh.
    4. **Chạy:** Bấm nút Chạy để đồng bộ.
    """)

# --- NAV ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    st.session_state['data_loaded'] = False 
    st.session_state['current_df'] = None

def go_to_list():
    clear_cache(); st.session_state['view'] = 'list'; st.session_state['selected_block_id'] = None

# ==========================================
# VIEW: LIST (DANH SÁCH KHỐI)
# ==========================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    c1, c2, c3, c4, c5 = st.columns([3.5, 1.5, 1.2, 0.8, 1.2]) 
    c1.caption("Quản lý các khối dữ liệu và lịch chạy tự động.")
    
    # 1. NÚT CHẠY TẤT CẢ (VÒNG LẶP TOÀN BỘ)
    if c2.button("▶️ CHẠY TẤT CẢ", type="primary"):
        all_blocks = get_cached_blocks()
        if not all_blocks: st.warning("Trống.")
        else:
            ctr = st.status("🚀 Đang chạy toàn bộ...", expanded=True)
            prog = ctr.progress(0, text="Init...")
            tot = len(all_blocks)
            for idx, block in enumerate(all_blocks):
                bid, bname = block['Block ID'], block['Block Name']
                prog.progress(int((idx/tot)*100), text=f"Xử lý: {bname}")
                ctr.write(f"📦 **{bname}**")
                links = be.get_links_by_block(st.secrets, bid)
                for l in links:
                    if l.get('Status') == "Đã chốt": continue
                    sname = l.get('Sheet Name')
                    # Parse date
                    ds, de = None, None
                    try:
                        if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                        if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                    except: pass
                    
                    data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                    if msg == "Success":
                        r_str, w_msg = be.process_data_final_v11(st.secrets, l['Link Sheet'], sname, bid, l['Link ID'], data, l.get('Status'))
                        if "Error" not in w_msg:
                            be.update_link_last_range(st.secrets, l['Link ID'], bid, r_str)
                            # GHI LOG
                            be.log_execution_history(st.secrets, bname, sname, "Thủ công (All)", "Success", r_str, "OK")
                            ctr.write(f"&nbsp;&nbsp;✅ {sname}: {r_str}")
                        else:
                            be.log_execution_history(st.secrets, bname, sname, "Thủ công (All)", "Error", "Fail", w_msg)
                            ctr.error(f"&nbsp;&nbsp;❌ {sname}: {w_msg}")
                    else:
                        be.log_execution_history(st.secrets, bname, sname, "Thủ công (All)", "Error", "Fail", msg)
                        ctr.error(f"&nbsp;&nbsp;❌ {sname}: API Fail")
                    time.sleep(0.5)
            prog.progress(100, text="Xong!"); ctr.update(label="✅ Hoàn tất!", state="complete", expanded=True); st.balloons()

    if c3.button("📖 Tài liệu HD"): show_user_guide()
    if c4.button("🔄 Reload"): clear_cache(); st.rerun()
    with c5:
        with st.popover("➕ Thêm Khối", use_container_width=True):
            if st.button("Tạo ngay") and (nn := st.text_input("Tên Khối")):
                be.create_block(st.secrets, nn); clear_cache(); st.success("OK"); time.sleep(0.5); st.rerun()

    blocks = get_cached_blocks()
    if blocks:
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 3, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                col2.info(format_schedule_display(b.get('Schedule Type'), b.get('Schedule Config')))
                
                # 2. NÚT CHẠY KHỐI LẺ
                if col3.button("▶️ Chạy Khối Này", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    with st.status(f"Đang chạy {b['Block Name']}...", expanded=True):
                        for l in links:
                            if l.get('Status') == "Đã chốt": continue
                            st.write(f"🔄 {l.get('Sheet Name')}")
                            ds, de = None, None
                            try:
                                if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                                if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                            except: pass
                            data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                            if msg == "Success":
                                r_str, w_msg = be.process_data_final_v11(st.secrets, l['Link Sheet'], l['Sheet Name'], b['Block ID'], l['Link ID'], data, l.get('Status'))
                                if "Error" not in w_msg:
                                    be.update_link_last_range(st.secrets, l['Link ID'], b['Block ID'], r_str)
                                    # GHI LOG
                                    be.log_execution_history(st.secrets, b['Block Name'], l.get('Sheet Name'), "Thủ công (Block)", "Success", r_str, "OK")
                                    st.write(f"✅ Xong: {r_str}")
                                else:
                                    be.log_execution_history(st.secrets, b['Block Name'], l.get('Sheet Name'), "Thủ công (Block)", "Error", "Fail", w_msg)
                                    st.error(f"Lỗi: {w_msg}")
                            else:
                                be.log_execution_history(st.secrets, b['Block Name'], l.get('Sheet Name'), "Thủ công (Block)", "Error", "Fail", msg)
                                st.error(f"Lỗi API: {msg}")
                    st.success("Xong!")

                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"): go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"): be.delete_block(st.secrets, b['Block ID']); clear_cache(); st.rerun()

# ==========================================
# VIEW: DETAIL (CHI TIẾT & CẤU HÌNH)
# ==========================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    # --- PHẦN HẸN GIỜ (ĐÃ KHÔI PHỤC ĐẦY ĐỦ) ---
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

        if st.button("💾 Lưu Cấu Hình Lịch", type="primary"):
            be.update_block_config_and_schedule(st.secrets, b_id, b_name, freq, sch_config)
            st.success("✅ Đã lưu cấu hình lịch!")
            time.sleep(1)

    st.divider()
    
    # 1. LOAD DATA
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
    edited_df = st.data_editor(st.session_state['current_df'], key="link_editor", use_container_width=True, hide_index=True,
        column_config={
            "Link ID": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "Block ID": st.column_config.TextColumn("Block", disabled=True, width="small"),
            "API URL": st.column_config.TextColumn("API URL", width="medium"),
            "Access Token": st.column_config.TextColumn("Token", width="small"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link", width="medium"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD-MM-YYYY", width="medium"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD-MM-YYYY", width="medium"),
            "Last Range": st.column_config.TextColumn("Range", disabled=True, width="medium"),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Cập nhật dữ liệu cũ", "Cập nhật dữ liệu mới", "Đã chốt"], width="medium", required=True),
        }
    )

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
    if c1.button("💾 LƯU DANH SÁCH", type="primary"):
        try:
            d = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d))
            st.session_state['current_df'] = edited_df
            st.success("✅ Đã lưu!"); time.sleep(1); st.rerun()
        except Exception as e: st.error(str(e))

    # 3. NÚT CHẠY TRONG CHI TIẾT (AUTO SAVE)
    if c2.button("🚀 LƯU & CHẠY NGAY", type="secondary"):
        try:
            d_run = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d_run)) # Auto Save
            st.toast("✅ Đã lưu cấu hình!")
        except Exception as e: st.error(str(e)); st.stop()

        valid = [r for r in d_run if r.get('Status') != "Đã chốt"]
        if not valid: st.warning("Không có link.")
        else:
            prog = st.progress(0, text="Chạy...")
            tot = len(valid)
            for i, l in enumerate(valid):
                stt = l.get('Status')
                prog.progress(int(((i)/tot)*100), text=f"Chạy: {l.get('Sheet Name')}")
                ds, de = None, None
                try: 
                    if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                    if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                except: pass
                
                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                if msg == "Success":
                    r_str, w_msg = be.process_data_final_v11(st.secrets, l['Link Sheet'], l['Sheet Name'], b_id, l['Link ID'], data, stt)
                    if "Error" not in w_msg:
                        be.update_link_last_range(st.secrets, l['Link ID'], b_id, r_str)
                        # GHI LOG
                        be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Success", r_str, "OK")
                        try:
                            lid = str(l['Link ID']).strip()
                            msk = st.session_state['current_df']['Link ID'].astype(str).str.strip() == lid
                            if msk.any():
                                ix = st.session_state['current_df'].index[msk][0]
                                st.session_state['current_df'].at[ix, 'Last Range'] = r_str
                        except: pass
                    else:
                        be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Error", "Fail", w_msg)
                        st.error(f"Lỗi: {w_msg}")
                else:
                    be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Error", "Fail", msg)
                    st.error(f"API Lỗi: {msg}")
                time.sleep(1)
            prog.progress(100, text="Xong!"); st.success("OK"); time.sleep(1); st.rerun()
