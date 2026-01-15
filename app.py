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
    "admin": "admin2025",
    "kinkin": "kinkin2025",
    "user": "user123"
}

if 'authenticated' not in st.session_state: st.session_state['authenticated'] = False
if 'user_role' not in st.session_state: st.session_state['user_role'] = ""
if 'show_log' not in st.session_state: st.session_state['show_log'] = False 

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

# --- HÀM LẤY LOG ---
def get_logs_data():
    try:
        sh, _ = be.get_connection(st.secrets)
        wks = sh.worksheet("log_lan_thuc_thi")
        data = wks.get_all_records()
        df = pd.DataFrame(data)
        if not df.empty: return df.iloc[::-1] 
        return df
    except: return pd.DataFrame()

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
    
    ## 3. THAO TÁC
    1. **Tạo Khối:** Thêm khối mới.
    2. **Cấu hình:** Nhập API, Token, Sheet Link.
    3. **Chạy:** Bấm nút Chạy để đồng bộ.
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
    
    # Chia cột cho menu chính
    c1, c2, c3, c4, c5 = st.columns([3, 1.3, 1.3, 1.3, 1]) 
    
    c1.caption("Quản lý các khối dữ liệu và lịch chạy tự động.")

    # 1. NÚT CHẠY TẤT CẢ
    if c2.button("▶️ CHẠY TẤT CẢ", type="primary"):
        all_blocks = get_cached_blocks()
        if not all_blocks: st.warning("Trống.")
        else:
            with st.status("🚀 Đang chạy toàn bộ hệ thống...", expanded=True) as status:
                ctr = st.container()
                for b in all_blocks:
                    bid, bname = b['Block ID'], b['Block Name']
                    ctr.write(f"**📦 Khối: {bname}**")
                    links = be.get_links_by_block(st.secrets, bid)
                    for l in links:
                        if l.get('Status') == "Đã chốt": continue
                        sname = l['Sheet Name']
                        
                        # Xử lý Link sạch
                        raw_url_run = l['Link Sheet']
                        if "docs.google.com" in str(raw_url_run):
                            try:
                                fid = str(raw_url_run).split("/d/")[1].split("/")[0]
                                final_link = f"https://docs.google.com/spreadsheets/d/{fid}"
                            except: final_link = raw_url_run
                        else: final_link = raw_url_run

                        ds, de = None, None
                        try:
                            if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                            if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                        except: pass
                        
                        data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                        if msg == "Success":
                            r_str, w_msg = be.process_data_final_v11(st.secrets, final_link, sname, bid, l['Link ID'], data, l.get('Status'))
                            if "Error" not in w_msg:
                                be.update_link_last_range(st.secrets, l['Link ID'], bid, r_str)
                                be.log_execution_history(st.secrets, bname, sname, "Thủ công (All)", "Success", r_str, "OK")
                                ctr.write(f"&nbsp;&nbsp;✅ {sname}: {r_str}")
                            else:
                                be.log_execution_history(st.secrets, bname, sname, "Thủ công (All)", "Error", "Fail", w_msg)
                                ctr.error(f"&nbsp;&nbsp;❌ {sname}: {w_msg}")
                        else:
                            be.log_execution_history(st.secrets, bname, sname, "Thủ công (All)", "Error", "Fail", msg)
                            ctr.error(f"&nbsp;&nbsp;❌ {sname}: {msg}")
                status.update(label="✅ Đã chạy xong!", state="complete", expanded=False)
                time.sleep(1)

    # 2. NÚT XEM LỊCH SỬ
    if c3.button("📜 XEM LỊCH SỬ"):
        st.session_state['show_log'] = not st.session_state['show_log']

    # 3. NÚT HƯỚNG DẪN
    if c4.button("📘 TÀI LIỆU HD"):
        show_user_guide()
    
    # Reload
    if c5.button("🔄"): clear_cache(); st.rerun()

    # --- KHU VỰC HIỂN THỊ LOG (POPUP DƯỚI NÚT) ---
    if st.session_state['show_log']:
        st.info("Đang tải nhật ký hoạt động...")
        df_log = get_logs_data()
        if not df_log.empty:
            st.dataframe(
                df_log, 
                use_container_width=True, 
                height=300,
                column_config={
                    "Time": st.column_config.TextColumn("Thời gian", width="medium"),
                    "Status": st.column_config.TextColumn("Trạng thái", width="small"),
                    "Message": st.column_config.TextColumn("Chi tiết", width="large"),
                }
            )
        else:
            st.warning("Chưa có lịch sử chạy nào.")
        st.markdown("---")

    st.divider()
    
    # --- KHU VỰC TẠO KHỐI MỚI ---
    with st.expander("➕ Tạo Khối Mới", expanded=False):
        with st.form("new_block"):
            new_name = st.text_input("Tên Khối (VD: Doanh Số, Nhân Sự)")
            if st.form_submit_button("Tạo ngay"):
                if new_name:
                    be.create_block(st.secrets, new_name)
                    clear_cache(); st.rerun()

    # --- DANH SÁCH KHỐI ---
    blocks = get_cached_blocks()
    if blocks:
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 3, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                col2.info(format_schedule_display(b.get('Schedule Type'), b.get('Schedule Config')))
                
                if col3.button("▶️ Chạy Khối Này", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    with st.status(f"Đang chạy {b['Block Name']}...", expanded=True):
                        for l in links:
                            if l.get('Status') == "Đã chốt": continue
                            st.write(f"🔄 {l.get('Sheet Name')}")
                            
                            # Xử lý Link sạch
                            raw_url_run = l['Link Sheet']
                            if "docs.google.com" in str(raw_url_run):
                                try:
                                    fid = str(raw_url_run).split("/d/")[1].split("/")[0]
                                    final_link = f"https://docs.google.com/spreadsheets/d/{fid}"
                                except: final_link = raw_url_run
                            else: final_link = raw_url_run

                            ds, de = None, None
                            try:
                                if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                                if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                            except: pass
                            
                            data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                            if msg == "Success":
                                r_str, w_msg = be.process_data_final_v11(st.secrets, final_link, l['Sheet Name'], b['Block ID'], l['Link ID'], data, l.get('Status'))
                                if "Error" not in w_msg:
                                    be.update_link_last_range(st.secrets, l['Link ID'], b['Block ID'], r_str)
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
                    if st.button("⚙️", key=f"dt_{b['Block ID']}"): go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️", key=f"dl_{b['Block ID']}", type="secondary"): be.delete_block(st.secrets, b['Block ID']); clear_cache(); st.rerun()

# ==========================================
# VIEW: DETAIL (CHI TIẾT & CẤU HÌNH)
# ==========================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    # --- PHẦN HẸN GIỜ ---
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
        
       # --- FIX: Chuẩn hóa ID để map token chính xác ---
        token_map = {}
        if not df_temp.empty:
            for _, row in df_temp.iterrows():
                # Làm sạch ID: Xóa khoảng trắng và đuôi .0 nếu có
                clean_id = str(row.get('Link ID', '')).strip().replace(".0", "")
                token_map[clean_id] = str(row.get('Access Token', '')).strip()
        st.session_state['original_token_map'] = token_map
        # -----------------------------------------------
        
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
    edited_df = st.data_editor(st.session_state['current_df'], key="link_editor", use_container_width=True, hide_index=True, num_rows="dynamic",
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
        
        # 1. Chuẩn hóa ID để tìm trong map (xóa đuôi .0 nếu có)
        raw_id = str(d.get('Link ID', ''))
        lid = raw_id.strip().replace(".0", "")
        
        # 2. Xử lý Token
        curr_token = str(d.get('Access Token', '')).strip()
        
        # Nếu đang hiển thị "Đã lưu..." -> Lấy token gốc từ map
        if "Đã lưu" in curr_token:
            d['Access Token'] = t_map.get(lid, "")
        else:
            # Nếu người dùng nhập mới -> Lấy giá trị nhập mới
            d['Access Token'] = curr_token

        d['Method'] = "GET"
        d['Block ID'] = bid 
        rows.append(d)
    return rows
   

    # --- KHU VỰC CÁC NÚT BẤM ---
    st.write("---")
    # Chia làm 4 cột để thêm nút Chạy Khối
    c1, c2, c3, c4 = st.columns([1.5, 1.5, 1.5, 2])

    # ==========================================
    # NÚT 1: LƯU DANH SÁCH (ĐÃ FIX LỖI DÒNG MỚI)
    # ==========================================
    if c1.button("💾 LƯU DANH SÁCH", type="primary", key="btn_save_list"):
        try:
            # 1. Tự động sinh ID cho dòng mới
            try:
                # Lấy max ID hiện có, bỏ qua các dòng lỗi/trống
                existing_ids = pd.to_numeric(edited_df['Link ID'], errors='coerce').dropna()
                next_id = int(existing_ids.max()) + 1 if not existing_ids.empty else 1
            except: next_id = 1

            # Duyệt qua từng dòng để điền ID nếu thiếu
            for idx in edited_df.index:
                # Lấy ID hiện tại, ép kiểu chuỗi và xóa khoảng trắng
                curr_id = str(edited_df.at[idx, 'Link ID']).strip()
                
                # Nếu ID trống, None, hoặc nan -> Gán ID mới
                if not curr_id or curr_id.lower() in ['none', 'nan', '']:
                    edited_df.at[idx, 'Link ID'] = str(next_id)
                    next_id += 1
                
                # Luôn gán lại Block ID để đảm bảo không bị lạc
                edited_df.at[idx, 'Block ID'] = b_id 

            # 2. Chuẩn bị dữ liệu và Lưu (Dùng hàm prep_data đã fix Token ở bước trước)
            d = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d))
            
            # 3. Reset để load lại dữ liệu mới nhất từ DB
            st.session_state['data_loaded'] = False 
            st.toast("✅ Đã lưu thành công!", icon="💾")
            time.sleep(1)
            st.rerun()
        except Exception as e: st.error(f"Lỗi khi lưu: {str(e)}")

    # ==========================================
    # NÚT 2: QUÉT QUYỀN (GIỮ NGUYÊN)
    # ==========================================
    if c2.button("🔍 QUÉT QUYỀN", key="btn_check_perm"):
        links_to_check = prep_data(edited_df, st.session_state['original_token_map'], b_id)
        failures = [] 
        bot_email_detected = ""

        with st.status("Đang kiểm tra quyền truy cập...", expanded=True) as status:
            for l in links_to_check:
                raw_url = str(l.get("Link Sheet", "")).strip()
                sheet_name = l.get("Sheet Name", "Không tên")
                if "http" not in raw_url and "http" in str(l.get("Sheet Name", "")):
                    raw_url = str(l.get("Sheet Name", "")).strip()

                if "docs.google.com" not in raw_url: continue 
                
                try:
                    if "/d/" in raw_url: file_id = raw_url.split("/d/")[1].split("/")[0]
                    else: file_id = raw_url
                    clean_url = f"https://docs.google.com/spreadsheets/d/{file_id}"
                except:
                    st.warning(f"⚠️ Link sai: {sheet_name}")
                    continue

                st.write(f"Checking: {sheet_name} ...")
                is_ok, msg, email_used = be.check_sheet_access(st.secrets, clean_url)
                if email_used: bot_email_detected = email_used
                
                if not is_ok:
                    failures.append((clean_url, msg))
                    st.error(f"❌ {sheet_name}: LỖI ({msg})")
                else:
                    st.write(f"✅ {sheet_name}: OK")
            
            if failures: status.update(label="⚠️ Có lỗi quyền truy cập!", state="error", expanded=False)
            else: status.update(label="✅ Tất cả OK!", state="complete", expanded=False)

        if failures:
            if not bot_email_detected: 
                try: bot_email_detected = st.secrets["gcp_service_account"]["client_email"]
                except: bot_email_detected = "bot-email-service-account"
            st.warning("👉 Hãy cấp quyền **Editor** cho email sau:")
            st.code(bot_email_detected, language="text")

    # ==========================================
    # NÚT 3 (MỚI): CHẠY KHỐI NÀY
    # ==========================================
    if c3.button("▶️ CHẠY KHỐI (Đã Lưu)", key="btn_run_block_detail"):
        # Lấy lại link từ DB để đảm bảo chạy dữ liệu đã lưu
        db_links = be.get_links_by_block(st.secrets, b_id)
        
        if not db_links:
            st.warning("Khối này chưa có link nào được lưu.")
        else:
            with st.status(f"🚀 Đang chạy khối: {b_name}...", expanded=True) as status:
                for l in db_links:
                    if l.get('Status') == "Đã chốt": continue
                    
                    st.write(f"🔄 Đang xử lý: **{l.get('Sheet Name')}**")
                    
                    # Xử lý Link Google Sheet
                    raw_url_run = l['Link Sheet']
                    if "docs.google.com" in str(raw_url_run):
                        try:
                            fid = str(raw_url_run).split("/d/")[1].split("/")[0]
                            final_link = f"https://docs.google.com/spreadsheets/d/{fid}"
                        except: final_link = raw_url_run
                    else: final_link = raw_url_run

                    # Xử lý ngày tháng
                    ds, de = None, None
                    try:
                        if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                        if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                    except: pass
                    
                    # Gọi API
                    data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                    
                    if msg == "Success":
                        # Ghi vào Sheet
                        r_str, w_msg = be.process_data_final_v11(st.secrets, final_link, l['Sheet Name'], b_id, l['Link ID'], data, l.get('Status'))
                        
                        if "Error" not in w_msg:
                            be.update_link_last_range(st.secrets, l['Link ID'], b_id, r_str)
                            be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Success", r_str, "OK")
                            st.write(f"✅ Thành công: {r_str}")
                        else:
                            be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Error", "Fail", w_msg)
                            st.error(f"❌ Lỗi ghi Sheet: {w_msg}")
                    else:
                        be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Error", "Fail", msg)
                        st.error(f"❌ Lỗi API: {msg}")
                    
                    time.sleep(0.5)
                status.update(label="✅ Đã chạy xong khối!", state="complete", expanded=False)
            st.success("Hoàn tất quy trình chạy.")

    # ==========================================
    # NÚT 4: LƯU & CHẠY NGAY (GIỮ NGUYÊN)
    # ==========================================
    if c4.button("🚀 LƯU & CHẠY CÁC DÒNG NÀY", type="secondary", key="btn_save_run"):
        # (Giữ nguyên code cũ của nút này ở phiên bản trước, hoặc copy logic lưu ở trên xuống đây nếu muốn đồng bộ)
        # Để code gọn, tôi khuyến nghị dùng nút Lưu riêng và Chạy riêng. 
        # Nhưng nếu muốn giữ, hãy đảm bảo logic sinh ID giống hệt nút Lưu ở trên.
        try:
            # 1. Logic sinh ID (Copy từ nút Lưu)
            try:
                existing_ids = pd.to_numeric(edited_df['Link ID'], errors='coerce').dropna()
                next_id = int(existing_ids.max()) + 1 if not existing_ids.empty else 1
            except: next_id = 1
            for idx in edited_df.index:
                curr_id = str(edited_df.at[idx, 'Link ID']).strip()
                if not curr_id or curr_id.lower() in ['none', 'nan', '']:
                    edited_df.at[idx, 'Link ID'] = str(next_id)
                    edited_df.at[idx, 'Block ID'] = b_id 
                    next_id += 1

            d_run = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d_run)) 
            st.toast("✅ Đã lưu cấu hình tạm thời!")
        except Exception as e: st.error(str(e)); st.stop()

        # Phần chạy (Giữ nguyên logic cũ của bạn)
        valid = [r for r in d_run if r.get('Status') != "Đã chốt"]
        if not valid: st.warning("Không có link nào để chạy.")
        else:
            prog = st.progress(0, text="Đang khởi động...")
            tot = len(valid)
            for i, l in enumerate(valid):
                stt = l.get('Status')
                prog.progress(int(((i)/tot)*100), text=f"Chạy: {l.get('Sheet Name')}")
                ds, de = None, None
                try: 
                    if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                    if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                except: pass
                
                # ... (Logic xử lý link giống các phần trên) ...
                raw_url_run = l['Link Sheet']
                if "docs.google.com" in str(raw_url_run):
                    try:
                        fid = str(raw_url_run).split("/d/")[1].split("/")[0]
                        final_link = f"https://docs.google.com/spreadsheets/d/{fid}"
                    except: final_link = raw_url_run
                else: final_link = raw_url_run

                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                if msg == "Success":
                    r_str, w_msg = be.process_data_final_v11(st.secrets, final_link, l['Sheet Name'], b_id, l['Link ID'], data, stt)
                    if "Error" not in w_msg:
                        be.update_link_last_range(st.secrets, l['Link ID'], b_id, r_str)
                        be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Success", r_str, "OK")
                    else:
                        be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Error", "Fail", w_msg)
                        st.error(f"Lỗi: {w_msg}")
                else:
                    be.log_execution_history(st.secrets, b_name, l.get('Sheet Name'), "Thủ công (Detail)", "Error", "Fail", msg)
                    st.error(f"API Lỗi: {msg}")
                time.sleep(0.5)
            
            st.session_state['data_loaded'] = False 
            prog.progress(100, text="Hoàn tất!"); st.success("Xong!"); time.sleep(1); st.rerun()
