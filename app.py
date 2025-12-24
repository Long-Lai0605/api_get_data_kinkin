import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

# --- CONFIG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# --- LOGIN ---
CREDENTIALS = {"admin": "admin888", "kinkin": "kinkin2025", "user": "user123"}
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

# --- CACHE ---
@st.cache_data(ttl=300)
def get_cached_blocks(): return be.get_all_blocks(st.secrets)
def clear_cache(): st.cache_data.clear()

# --- HELPER ---
def format_schedule_display(sch_type, sch_config_str):
    if sch_type == "Thủ công": return "Thủ công"
    try:
        cfg = json.loads(sch_config_str) if isinstance(sch_config_str, str) else sch_config_str
        if sch_type == "Hàng ngày": return f"📅 Hàng ngày | {cfg.get('fixed_time','')}"
        elif sch_type == "Hàng tuần": return "🗓️ Hàng tuần"
        elif sch_type == "Hàng tháng": return "📆 Hàng tháng"
    except: return sch_type
    return sch_type

@st.dialog("📖 TÀI LIỆU HƯỚNG DẪN SỬ DỤNG", width="large")
def show_user_guide():
    st.markdown("""
    ## HƯỚNG DẪN NHANH
    1. **Tạo Khối:** Nhấn 'Thêm Khối' -> Nhập tên.
    2. **Cấu hình:** Nhấn 'Chi tiết' -> Nhập API URL, Token, Sheet Link.
    3. **Chạy:** Nhấn nút Chạy để đồng bộ dữ liệu.
    *(Xem chi tiết trong tài liệu nội bộ)*
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
# VIEW: LIST
# ==========================================
if st.session_state['view'] == 'list':
    st.title("⚡ QUẢN LÝ KHỐI DỮ LIỆU")
    c1, c2, c3, c4, c5 = st.columns([3.5, 1.5, 1.2, 0.8, 1.2]) 
    c1.caption("Quản lý các khối dữ liệu và lịch chạy tự động.")
    
    # 1. CHẠY TẤT CẢ
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
                            be.log_execution_history(st.secrets, bname, "Thủ công (All)", "Success", f"{sname}: {r_str}") # <--- LOG
                            ctr.write(f"&nbsp;&nbsp;✅ {sname}: {r_str}")
                        else:
                            be.log_execution_history(st.secrets, bname, "Thủ công (All)", "Error", f"{sname}: {w_msg}") # <--- LOG
                            ctr.error(f"&nbsp;&nbsp;❌ {sname}: {w_msg}")
                    else:
                        be.log_execution_history(st.secrets, bname, "Thủ công (All)", "Error", f"{sname}: API Fail") # <--- LOG
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
                
                # 2. CHẠY KHỐI LẺ
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
                                    be.log_execution_history(st.secrets, b['Block Name'], "Thủ công (Block)", "Success", f"{l.get('Sheet Name')}: {r_str}") # <--- LOG
                                    st.write(f"✅ Xong: {r_str}")
                                else:
                                    be.log_execution_history(st.secrets, b['Block Name'], "Thủ công (Block)", "Error", f"{l.get('Sheet Name')}: {w_msg}") # <--- LOG
                                    st.error(f"Lỗi: {w_msg}")
                            else:
                                be.log_execution_history(st.secrets, b['Block Name'], "Thủ công (Block)", "Error", f"{l.get('Sheet Name')}: {msg}") # <--- LOG
                                st.error(f"Lỗi API: {msg}")
                    st.success("Xong!")

                with col4:
                    if st.button("⚙️ Chi tiết", key=f"dt_{b['Block ID']}"): go_to_detail(b['Block ID'], b['Block Name']); st.rerun()
                    if st.button("🗑️ Xóa", key=f"dl_{b['Block ID']}", type="secondary"): be.delete_block(st.secrets, b['Block ID']); clear_cache(); st.rerun()

# ==========================================
# VIEW: DETAIL
# ==========================================
elif st.session_state['view'] == 'detail':
    b_id = st.session_state['selected_block_id']
    b_name = st.session_state['selected_block_name']
    c_back, c_tit = st.columns([1, 6])
    if c_back.button("⬅️ Quay lại"): go_to_list(); st.rerun()
    c_tit.title(f"⚙️ {b_name}")
    
    with st.expander("⏰ Cài đặt Lịch chạy", expanded=True):
        freq = st.radio("Tần suất", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
        # (Giữ nguyên code config lịch như cũ để tiết kiệm chỗ hiển thị ở đây...)
        sch_config = {} 
        # ... (Code config lịch của bạn ở đây) ...
        if st.button("💾 Lưu Cấu Hình Lịch", type="primary"):
            be.update_block_config_and_schedule(st.secrets, b_id, b_name, freq, sch_config)
            st.success("✅ Đã lưu!"); time.sleep(1)

    st.divider()
    
    if not st.session_state['data_loaded']:
        original_links = be.get_links_by_block(st.secrets, b_id)
        # (Code load data giữ nguyên...)
        # ...
        st.session_state['data_loaded'] = True
        # Giả lập load xong để code ngắn gọn
    
    # Fake editor display for context
    if 'current_df' not in st.session_state or st.session_state['current_df'] is None:
         # Fallback empty
         st.session_state['current_df'] = pd.DataFrame(columns=["Link ID", "Block ID", "Status"])

    edited_df = st.data_editor(st.session_state['current_df'], key="link_editor", use_container_width=True)

    def prep_data(df, t_map, bid):
        rows = []
        for _, r in df.iterrows():
            d = r.to_dict()
            # ... (Code prepare data giữ nguyên)
            rows.append(d)
        return rows

    c1, c2 = st.columns([1, 4])
    if c1.button("💾 LƯU DANH SÁCH", type="primary"):
        # ... (Code Save giữ nguyên)
        pass

    # 3. NÚT CHẠY TRONG CHI TIẾT
    if c2.button("🚀 LƯU & CHẠY NGAY", type="secondary"):
        try:
            d_run = prep_data(edited_df, st.session_state['original_token_map'], b_id)
            be.save_links_bulk(st.secrets, b_id, pd.DataFrame(d_run))
            st.toast("✅ Đã lưu!")
        except Exception as e: st.error(str(e)); st.stop()

        valid = [r for r in d_run if r.get('Status') != "Đã chốt"]
        if not valid: st.warning("Không có link.")
        else:
            prog = st.progress(0, text="Chạy...")
            tot = len(valid)
            for i, l in enumerate(valid):
                stt = l.get('Status')
                prog.progress(int(((i)/tot)*100), text=f"Chạy: {l.get('Sheet Name')}")
                # ... (Date parsing & Fetching giữ nguyên) ...
                # Giả sử đã fetch xong -> data
                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], None, None, None)
                
                if msg == "Success":
                    r_str, w_msg = be.process_data_final_v11(st.secrets, l['Link Sheet'], l['Sheet Name'], b_id, l['Link ID'], data, stt)
                    if "Error" not in w_msg:
                        be.update_link_last_range(st.secrets, l['Link ID'], b_id, r_str)
                        be.log_execution_history(st.secrets, b_name, "Thủ công (Detail)", "Success", f"{l.get('Sheet Name')}: {r_str}") # <--- LOG
                        # Update UI Local...
                    else:
                        be.log_execution_history(st.secrets, b_name, "Thủ công (Detail)", "Error", f"{l.get('Sheet Name')}: {w_msg}") # <--- LOG
                        st.error(f"Lỗi: {w_msg}")
                else:
                    be.log_execution_history(st.secrets, b_name, "Thủ công (Detail)", "Error", f"{l.get('Sheet Name')}: {msg}") # <--- LOG
                    st.error(f"API Lỗi: {msg}")
                time.sleep(1)
            prog.progress(100, text="Xong!"); st.success("OK"); time.sleep(1); st.rerun()
