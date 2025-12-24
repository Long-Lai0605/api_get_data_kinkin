import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- 1. CẤU HÌNH TRANG (BẮT BUỘC ĐẦU TIÊN) ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- 2. HỆ THỐNG ĐĂNG NHẬP (LOGIN SYSTEM) ---
# Danh sách tài khoản
CREDENTIALS = {
    "admin": "admin888",
    "kinkin": "kinkin2025",
    "user": "user123"
}

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'user_role' not in st.session_state:
    st.session_state['user_role'] = ""

def check_login():
    username = st.session_state['input_username']
    password = st.session_state['input_password']
    
    if username in CREDENTIALS and CREDENTIALS[username] == password:
        st.session_state['authenticated'] = True
        st.session_state['user_role'] = username
    else:
        st.error("❌ Sai tên đăng nhập hoặc mật khẩu!")

def logout():
    st.session_state['authenticated'] = False
    st.session_state['user_role'] = ""
    st.session_state['view'] = 'list' # Reset view
    st.rerun()

# --- GIAO DIỆN ĐĂNG NHẬP ---
if not st.session_state['authenticated']:
    st.markdown("<br><br>", unsafe_allow_html=True)
    col_l1, col_l2, col_l3 = st.columns([1, 2, 1])
    
    with col_l2:
        with st.container(border=True):
            st.title("🔒 KINKIN LOGIN")
            st.caption("Hệ thống quản trị dữ liệu tập trung")
            st.text_input("Tên đăng nhập", key="input_username")
            st.text_input("Mật khẩu", type="password", key="input_password")
            st.button("Đăng Nhập", type="primary", on_click=check_login, use_container_width=True)
            
            st.markdown("---")
            st.caption("Liên hệ Admin nếu quên mật khẩu.")
    
    st.stop() # DỪNG CHƯƠNG TRÌNH TẠI ĐÂY NẾU CHƯA LOGIN

# =========================================================
# PHẦN DƯỚI NÀY CHỈ CHẠY KHI ĐÃ LOGIN THÀNH CÔNG
# =========================================================

# --- SIDEBAR: HIỂN THỊ USER & LOGOUT ---
with st.sidebar:
    st.write(f"👤 Xin chào, **{st.session_state['user_role'].upper()}**")
    if st.button("Đăng xuất", type="secondary"):
        logout()

# --- STATE CHÍNH ---
if 'view' not in st.session_state: st.session_state['view'] = 'list'
if 'selected_block_id' not in st.session_state: st.session_state['selected_block_id'] = None
if 'selected_block_name' not in st.session_state: st.session_state['selected_block_name'] = ""
if 'data_loaded' not in st.session_state: st.session_state['data_loaded'] = False
if 'current_df' not in st.session_state: st.session_state['current_df'] = None
if 'original_token_map' not in st.session_state: st.session_state['original_token_map'] = {}

# --- INIT DB ---
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

# --- CACHING FUNCTIONS ---
@st.cache_data(ttl=300)
def get_cached_blocks():
    return be.get_all_blocks(st.secrets)

def clear_cache():
    st.cache_data.clear()

# --- HELPER: FORMAT LỊCH HIỂN THỊ ---
def format_schedule_display(sch_type, sch_config_str):
    if sch_type == "Thủ công": return "Thủ công (Chạy bằng tay)"
    try:
        cfg = json.loads(sch_config_str) if isinstance(sch_config_str, str) else sch_config_str
        if not cfg: return sch_type
        
        if sch_type == "Hàng ngày":
            details = []
            if "fixed_time" in cfg: details.append(f"Cố định: {cfg['fixed_time'][:5]}")
            if "loop_minutes" in cfg: details.append(f"Lặp mỗi {cfg['loop_minutes']}p")
            return f"📅 Hàng ngày | {', '.join(details)}"
            
        elif sch_type == "Hàng tuần":
            details = []
            if "run_1" in cfg: 
                r1 = cfg["run_1"]
                details.append(f"{r1.get('day')} {r1.get('time')[:5]}")
            if "run_2" in cfg: 
                r2 = cfg["run_2"]
                details.append(f"{r2.get('day')} {r2.get('time')[:5]}")
            return f"🗓️ Hàng tuần | {', '.join(details)}"
            
        elif sch_type == "Hàng tháng":
            details = []
            if "run_1" in cfg: 
                r1 = cfg["run_1"]
                details.append(f"Ngày {r1.get('day')} lúc {r1.get('time')[:5]}")
            if "run_2" in cfg: 
                r2 = cfg["run_2"]
                details.append(f"Ngày {r2.get('day')} lúc {r2.get('time')[:5]}")
            return f"📆 Hàng tháng | {', '.join(details)}"
            
    except: return sch_type
    return sch_type

# --- POPUP HƯỚNG DẪN SỬ DỤNG ---
@st.dialog("📖 TÀI LIỆU HƯỚNG DẪN SỬ DỤNG", width="large")
def show_user_guide():
    st.markdown("""
    ## 1. TỔNG QUAN & CÁC CHẾ ĐỘ CẬP NHẬT DỮ LIỆU
    Hệ thống **KINKIN MASTER ENGINE** giúp tự động lấy dữ liệu từ 1Office về Google Sheets. Điểm mạnh nhất là khả năng xử lý dữ liệu thông minh qua 4 chế độ:

    | Chế độ (Trạng thái) | Hành động của Robot | Khi nào nên dùng? |
    | :--- | :--- | :--- |
    | **1. Chưa chốt & đang cập nhật**<br>*(Replace Mode)* | **Xóa cũ - Thay mới:**<br>Robot xóa sạch dữ liệu cũ của Link này (dựa trên bộ lọc) và điền lại toàn bộ dữ liệu mới nhất. | Dữ liệu tháng hiện tại, biến động liên tục, cần làm mới hoàn toàn. |
    | **2. Cập nhật dữ liệu cũ**<br>*(Update Only)* | **Chỉ sửa cái đã có:**<br>Chỉ tìm ID đã tồn tại để cập nhật thông tin mới. **Tuyệt đối không thêm dòng mới.** | Danh sách đã chốt cứng, chỉ cần cập nhật trạng thái/tiến độ. |
    | **3. Cập nhật dữ liệu mới**<br>*(Append Only)* | **Chỉ thêm cái chưa có:**<br>Chỉ tìm ID mới tinh để điền thêm vào dưới cùng. **Giữ nguyên dòng cũ.** | Lưu trữ lịch sử, log dữ liệu tích lũy dần. |
    | **4. Đã chốt**<br>*(Skip)* | **Ngủ đông:**<br>Robot bỏ qua, không làm gì cả. Dữ liệu được bảo vệ an toàn tuyệt đối. | Dữ liệu các tháng trước đã quyết toán xong. |

    ---
    ## 2. GIỚI HẠN & TỐC ĐỘ XỬ LÝ (QUAN TRỌNG)
    *Do hệ thống chạy trên Cloud trung gian (Streamlit) kết nối giữa 1Office và Google, tốc độ phụ thuộc vào đường truyền quốc tế.*

    ### A. Thời gian xử lý ước tính (Thực tế)
    *Người dùng vui lòng kiên nhẫn và không tắt trình duyệt trong quá trình xử lý:*
    * **Dưới 1.000 dòng:** Mất khoảng **30 giây - 1 phút**.
    * **Khoảng 10.000 dòng:** Mất khoảng **3 - 5 phút**.
    * **Trên 50.000 dòng:** Mất khoảng **15 - 30 phút** (Có rủi ro quá tải).
    *(Khuyên dùng: Nên chia nhỏ dữ liệu bằng bộ lọc Filter Key để chạy từng phần).*

    ### B. Cơ chế "Xếp hình thông minh"
    1. **Khoanh vùng an toàn:** Khi cập nhật một phần dữ liệu (VD: Tháng 5), Robot sẽ "khóa" tất cả các tháng còn lại. Dữ liệu cũ được bảo vệ an toàn.
    2. **Sắp xếp trật tự:** Dữ liệu mới tải về được tự động sắp xếp lại đúng vị trí (theo ID). Không bị chèn đè lên nhau dù chạy lộn xộn.
    3. **Lưu ý Google Sheet:** Nếu Sheet đích chứa quá nhiều công thức (VLOOKUP, QUERY...), tốc độ sẽ rất chậm. -> **Khuyên dùng: Sheet nhận dữ liệu nên để trơn (chỉ chứa dữ liệu thô).**

    ---
    ## 3. CÁC BƯỚC THAO TÁC & CẤU HÌNH LỌC
    *Việc cấu hình Bộ lọc (Filter) là chìa khóa để hệ thống chạy nhanh và ổn định.*

    ### Bước 1: Tạo Khối & Nhập Liệu
    1. Tại màn hình chính, bấm nút **"➕ Thêm Khối Mới"** -> Nhập tên -> Tạo.
    2. Bấm nút **"⚙️ Chi tiết"** để vào bên trong khối.
    3. Nhập đầy đủ: API URL, Token (Access Token), Link Google Sheet, Tên Sheet.

    ### Bước 2: Cấu hình Lọc (Quan trọng)
    **Trường hợp A: Lấy dữ liệu theo khoảng thời gian (KHUYÊN DÙNG)**
    * Điền **Filter Key**: Tên trường ngày tháng (VD: `created_date`, `date_sign`...).
    * Điền **Từ ngày / Đến ngày**: Chọn khoảng thời gian cụ thể (VD: 01/10/2024 đến 31/10/2024).
    * -> *Robot chạy nhanh, chỉ xử lý đúng khoảng thời gian đó.*

    **Trường hợp B: Lấy TOÀN BỘ lịch sử (CẨN THẬN)**
    * **ĐỂ TRỐNG** ô Filter Key.
    * **ĐỂ TRỐNG** ô Từ ngày / Đến ngày.
    * -> *Robot tải tất cả dữ liệu. Chỉ dùng khi khởi tạo lần đầu. Rất chậm nếu >50k dòng.*

    ### Bước 3: Chọn Trạng thái & Lưu
    1. Tại cột **Trạng thái**, chọn chế độ phù hợp (VD: *Chưa chốt & đang cập nhật*).
    2. Bấm nút **"💾 LƯU DANH SÁCH"** (Màu đỏ) để lưu cấu hình.

    ### Bước 4: Chạy & Hẹn giờ
    * **Chạy ngay:** Bấm nút **"🚀 LƯU & CHẠY NGAY"** (Màu trắng) để bắt đầu đồng bộ. Theo dõi thanh tiến trình bên dưới.
    * **Hẹn giờ:** Mở mục **"⏰ Cài đặt Lịch chạy"**, chọn tần suất (Hàng ngày/Tuần) rồi bấm **"💾 Lưu Cấu Hình Lịch"**.
    """)

# --- NAV ---
def go_to_detail(b_id, b_name):
    st.session_state['selected_block_id'] = b_id
    st.session_state['selected_block_name'] = b_name
    st.session_state['view'] = 'detail'
    st.session_state['data_loaded'] = False 
    st.session_state['current_df'] = None

def go_to_list():
    clear_cache()
    st.session_state['view'] = 'list'
    st.session_state['selected_block_id'] = None

# ==========================================
# VIEW: LIST (DANH SÁCH)
# ==========================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    
    # Chia cột: [Caption] [Chạy Tất Cả] [HDSD] [Refresh] [Thêm Khối]
    c1, c2, c3, c4, c5 = st.columns([3.5, 1.5, 1.2, 0.8, 1.2]) 
    
    c1.caption("Quản lý các khối dữ liệu và lịch chạy tự động.")
    
    # --- NÚT CHẠY TẤT CẢ ---
    if c2.button("▶️ CHẠY TẤT CẢ", type="primary"):
        all_blocks = get_cached_blocks()
        if not all_blocks:
            st.warning("Chưa có khối dữ liệu nào.")
        else:
            status_container = st.status("🚀 Đang chạy toàn bộ hệ thống...", expanded=True)
            total_blocks = len(all_blocks)
            global_progress = status_container.progress(0, text="Khởi động...")
            
            for idx, block in enumerate(all_blocks):
                b_id = block['Block ID']
                b_name = block['Block Name']
                
                global_progress.progress(int((idx / total_blocks) * 100), text=f"Đang xử lý Khối {idx+1}/{total_blocks}: **{b_name}**")
                status_container.write(f"📦 **Bắt đầu khối: {b_name}**")
                
                links = be.get_links_by_block(st.secrets, b_id)
                if not links:
                    status_container.write(f"--- Khối {b_name} trống, bỏ qua.")
                    continue
                
                for l in links:
                    stt = l.get('Status')
                    if stt == "Đã chốt": continue
                    
                    sheet_name = l.get('Sheet Name')
                    ds, de = None, None
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
                            status_container.write(f"&nbsp;&nbsp;&nbsp;&nbsp;✅ {sheet_name}: {range_str}")
                        else:
                            status_container.error(f"&nbsp;&nbsp;&nbsp;&nbsp;❌ {sheet_name}: Lỗi ghi ({w_msg})")
                    else:
                        status_container.error(f"&nbsp;&nbsp;&nbsp;&nbsp;❌ {sheet_name}: Lỗi API ({msg})")
                    
                    time.sleep(0.5)
                
                status_container.write("---")
            
            global_progress.progress(100, text="Hoàn tất!")
            status_container.update(label="✅ Đã chạy xong tất cả các khối!", state="complete", expanded=True)
            st.balloons()

    # Nút Hướng Dẫn
    if c3.button("📖 Tài liệu HD"):
        show_user_guide()

    # Nút Refresh
    if c4.button("🔄 Reload"):
        clear_cache()
        st.rerun()

    # Nút Thêm Mới
    with c5:
        with st.popover("➕ Thêm Khối", use_container_width=True):
            new_name = st.text_input("Tên Khối")
            if st.button("Tạo ngay") and new_name:
                be.create_block(st.secrets, new_name)
                clear_cache()
                st.success("Đã tạo!")
                time.sleep(0.5); st.rerun()

    blocks = get_cached_blocks()
    
    if blocks:
        df_b = pd.DataFrame(blocks).drop_duplicates(subset=["Block ID"])
        blocks = df_b.to_dict('records')
    
    if not blocks: st.info("Chưa có dữ liệu.")
    else:
        st.write("---")
        for b in blocks:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 3, 2, 1])
                col1.subheader(f"📦 {b['Block Name']}")
                
                sch_display = format_schedule_display(b.get('Schedule Type'), b.get('Schedule Config', '{}'))
                col2.info(f"{sch_display}")
                
                if col3.button("▶️ Chạy Khối Này", key=f"run_{b['Block ID']}"):
                    links = be.get_links_by_block(st.secrets, b['Block ID'])
                    if not links: st.warning("Chưa có Link nào.")
                    else:
                        with st.status(f"Đang chạy khối {b['Block Name']}...", expanded=True):
                            for l in links:
                                stt = l.get('Status')
                                if stt == "Đã chốt": continue
                                st.write(f"🔄 {l.get('Sheet Name')}")
                                
                                ds, de = None, None
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
                        clear_cache()
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
    
    # LOAD DATA
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
    
    # EDITOR
    edited_df = st.data_editor(
        st.session_state['current_df'],
        column_config={
            "Link ID": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "Block ID": st.column_config.TextColumn("Block", disabled=True, width="small"),
            "API URL": st.column_config.TextColumn("API URL", width="medium"),
            "Access Token": st.column_config.TextColumn("Token", width="small"),
            "Link Sheet": st.column_config.LinkColumn("Sheet Link", width="medium"),
            "Sheet Name": st.column_config.TextColumn("Sheet Name", width="small"),
            "Filter Key": st.column_config.TextColumn("Filter Key", width="small"),
            "Date Start": st.column_config.DateColumn("Từ ngày", format="DD-MM-YYYY", width="medium"),
            "Date End": st.column_config.DateColumn("Đến ngày", format="DD-MM-YYYY", width="medium"),
            "Last Range": st.column_config.TextColumn("Range", disabled=True, width="medium"),
            "Status": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Cập nhật dữ liệu cũ", "Cập nhật dữ liệu mới", "Đã chốt"], width="medium", required=True),
        },
        use_container_width=True, num_rows="dynamic", key="link_editor", hide_index=True
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
                        try:
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
