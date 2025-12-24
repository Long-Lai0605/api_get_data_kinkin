import streamlit as st
import pandas as pd
import backend as be
import time
import json
from datetime import datetime

# --- CẤU HÌNH TRANG ---
st.set_page_config(page_title="KINKIN MASTER ENGINE", page_icon="⚡", layout="wide")

# --- CSS TÙY CHỈNH (GIAO DIỆN ĐẸP) ---
st.markdown("""
<style>
    .stButton>button {width: 100%; border-radius: 5px; height: 3em;}
    .reportview-container {margin-top: -2em;}
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    div[data-testid="stMetricValue"] {font-size: 1.2rem;}
</style>
""", unsafe_allow_html=True)

# --- KHỞI TẠO SESSION STATE ---
if 'show_log' not in st.session_state: st.session_state.show_log = False

# --- HÀM HỖ TRỢ ---
def load_secrets():
    return st.secrets

def get_logs(secrets):
    try:
        sh, _ = be.get_connection(secrets)
        wks = sh.worksheet("log_lan_thuc_thi")
        # Lấy toàn bộ log
        data = wks.get_all_records()
        df = pd.DataFrame(data)
        if not df.empty:
            # Sắp xếp thời gian (Mới nhất lên đầu) - Giả sử cột Time format chuẩn
            # Nếu không sort được thì hiển thị đảo ngược
            return df.iloc[::-1] 
        return df
    except Exception as e:
        st.error(f"Lỗi tải log: {e}")
        return pd.DataFrame()

# --- HEADER & MENU CHÍNH ---
st.title("⚡ KINKIN MASTER ENGINE")
st.markdown("---")

# MENU 3 NÚT: CHẠY ALL - XEM LOG - HƯỚNG DẪN
col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 1])

with col_btn1:
    if st.button("🚀 CHẠY TẤT CẢ (FORCE RUN)", type="primary"):
        with st.status("Đang kích hoạt toàn bộ hệ thống...", expanded=True) as status:
            secrets = load_secrets()
            blocks = be.get_all_blocks(secrets)
            for b in blocks:
                st.write(f"**Đang xử lý Khối: {b['Block Name']}...**")
                links = be.get_links_by_block(secrets, b['Block ID'])
                for l in links:
                    if l.get('Status') == "Đã chốt": continue
                    st.write(f"👉 Sheet: {l['Sheet Name']}")
                    # (Code xử lý giống run_headless nhưng có hiển thị UI)
                    ds, de = None, None
                    try:
                        if l.get('Date Start'): ds = pd.to_datetime(l['Date Start'], dayfirst=True).date()
                        if l.get('Date End'): de = pd.to_datetime(l['Date End'], dayfirst=True).date()
                    except: pass
                    
                    data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de)
                    if msg == "Success":
                        r_str, w_msg = be.process_data_final_v11(secrets, l['Link Sheet'], l['Sheet Name'], b['Block ID'], l['Link ID'], data, l.get('Status'))
                        if "Error" not in w_msg:
                            be.update_link_last_range(secrets, l['Link ID'], b['Block ID'], r_str)
                            be.log_execution_history(secrets, b['Block Name'], l['Sheet Name'], "Thủ công (Web)", "Success", r_str, "OK")
                            st.success(f"✅ {l['Sheet Name']}: Done ({r_str})")
                        else:
                            be.log_execution_history(secrets, b['Block Name'], l['Sheet Name'], "Thủ công (Web)", "Error", "Fail", w_msg)
                            st.error(f"❌ {l['Sheet Name']}: Lỗi ghi ({w_msg})")
                    else:
                        be.log_execution_history(secrets, b['Block Name'], l['Sheet Name'], "Thủ công (Web)", "Error", "Fail", msg)
                        st.error(f"❌ {l['Sheet Name']}: Lỗi API ({msg})")
            status.update(label="✅ Đã chạy xong toàn bộ!", state="complete", expanded=False)

with col_btn2:
    if st.button("📜 XEM LẦN THỰC THI"):
        # Toggle trạng thái hiển thị Log
        st.session_state.show_log = not st.session_state.show_log

with col_btn3:
    with st.expander("📘 HƯỚNG DẪN SỬ DỤNG"):
        st.markdown("""
        **1. Quản lý Khối (Blocks):** Tạo các nhóm dữ liệu (VD: Doanh Số, Nhân Sự).
        **2. Quản lý Link:** Thêm các API 1Office vào từng khối.
        **3. Lịch Trình:**
           - **Thủ công:** Chỉ chạy khi bạn bấm nút.
           - **Tự động:** Bot GitHub sẽ chạy ngầm (10p/lần).
        **4. Ý nghĩa Log:**
           - **Success:** Chạy ngon.
           - **Error:** Có lỗi (Xem cột Message để sửa).
        """)

# --- KHU VỰC HIỂN THỊ LOG (POPUP) ---
if st.session_state.show_log:
    st.info("dang tải dữ liệu lịch sử...")
    secrets = load_secrets()
    df_log = get_logs(secrets)
    
    st.subheader("📜 Nhật ký hoạt động (Mới nhất)")
    if not df_log.empty:
        # Format màu sắc cho đẹp
        def highlight_status(val):
            color = '#d4edda' if val == 'Success' else '#f8d7da' if val == 'Error' else ''
            return f'background-color: {color}'

        # Hiển thị bảng
        st.dataframe(
            df_log.style.applymap(highlight_status, subset=['Status']),
            use_container_width=True,
            height=300
        )
        if st.button("Đóng Log"):
            st.session_state.show_log = False
            st.rerun()
    else:
        st.warning("Chưa có dữ liệu lịch sử nào.")
    st.markdown("---")


# --- PHẦN CHÍNH: QUẢN LÝ BLOCKS & LINKS ---
secrets = load_secrets()
blocks = be.get_all_blocks(secrets)

if not blocks:
    st.warning("Chưa có Khối nào. Hãy tạo Khối đầu tiên!")
    with st.form("create_first_block"):
        new_name = st.text_input("Tên Khối Mới (VD: Data Sales)")
        if st.form_submit_button("Tạo Khối"):
            be.create_block(secrets, new_name)
            st.rerun()
else:
    # Sidebar chọn Block
    block_names = [b['Block Name'] for b in blocks]
    selected_block_name = st.sidebar.selectbox("📂 CHỌN KHỐI DỮ LIỆU", block_names)
    
    # Tìm ID của Block đang chọn
    current_block = next((b for b in blocks if b['Block Name'] == selected_block_name), None)
    b_id = current_block['Block ID']

    st.header(f"📂 Khối: {selected_block_name}")
    
    # Tab quản lý
    tab1, tab2, tab3 = st.tabs(["🔗 Danh sách Link", "⚙️ Cấu hình Lịch chạy", "❌ Xóa Khối"])

    # TAB 1: DANH SÁCH LINK
    with tab1:
        links = be.get_links_by_block(secrets, b_id)
        if links:
            df_links = pd.DataFrame(links)
            # Chọn cột hiển thị cho gọn
            show_cols = ["Link ID", "Sheet Name", "API URL", "Status", "Last Range"]
            st.dataframe(df_links[show_cols], use_container_width=True)
        else:
            st.info("Chưa có Link nào trong khối này.")

        with st.expander("➕ THÊM / CẬP NHẬT LINK (Bulk Upload)"):
            st.markdown("""
            **Paste dữ liệu từ Excel (Cột: API URL | Access Token | Link Sheet | Sheet Name | Filter Key | Date Start | Date End | Status)**
            """)
            raw_data = st.text_area("Dán dữ liệu vào đây:", height=150)
            if st.button("Lưu Danh Sách Link"):
                try:
                    # Xử lý dữ liệu paste từ Excel
                    rows = [r.split('\t') for r in raw_data.strip().split('\n')]
                    df_new = pd.DataFrame(rows, columns=["API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"])
                    
                    # Tự động sinh ID
                    df_new["Link ID"] = [str(i+1) for i in range(len(df_new))]
                    df_new["Block ID"] = b_id # Gán ID Block hiện tại
                    
                    # Bổ sung các cột thiếu
                    for c in ["Method", "Last Range"]: df_new[c] = ""
                    
                    be.save_links_bulk(secrets, b_id, df_new)
                    st.success("Đã lưu thành công!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Lỗi định dạng: {e}")

    # TAB 2: CẤU HÌNH LỊCH
    with tab2:
        st.subheader("⏰ Hẹn giờ chạy tự động")
        current_sch_type = current_block.get('Schedule Type', 'Thủ công')
        current_config = current_block.get('Schedule Config', '{}')
        
        col_sch1, col_sch2 = st.columns(2)
        new_sch_type = col_sch1.selectbox("Loại Lịch", ["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"], index=["Thủ công", "Hàng ngày", "Hàng tuần", "Hàng tháng"].index(current_sch_type))
        
        config_input = col_sch2.text_area("Cấu hình JSON (Nâng cao)", value=str(current_config), height=100)
        
        # Helper tạo JSON nhanh
        st.markdown("---")
        st.markdown("**🛠 Công cụ tạo JSON nhanh:**")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Mẫu: Hàng ngày (08:00)"):
                config_input = '{"fixed_time": "08:00:00"}'
            if st.button("Mẫu: Lặp lại (60p)"):
                config_input = '{"loop_minutes": 60}'
        with c2:
            if st.button("Mẫu: Thứ 2 hàng tuần"):
                config_input = '{"run_1": {"day": "Thứ 2", "time": "08:00:00"}}'
            if st.button("Mẫu: Ngày 1 hàng tháng"):
                config_input = '{"run_1": {"day": 1, "time": "08:00:00"}}'
        
        if st.button("💾 Lưu Cấu Hình Lịch"):
            try:
                # Validate JSON
                clean_json = json.loads(config_input) if isinstance(config_input, str) else config_input
                be.update_block_config_and_schedule(secrets, b_id, selected_block_name, new_sch_type, clean_json)
                st.success("Đã lưu cấu hình!")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Lỗi JSON: {e}")

    # TAB 3: XÓA BLOCK
    with tab3:
        st.warning("Hành động này sẽ xóa toàn bộ Link trong khối!")
        if st.button("🗑 Xóa Khối Này", type="primary"):
            be.delete_block(secrets, b_id)
            st.success("Đã xóa khối!")
            time.sleep(1)
            st.rerun()
