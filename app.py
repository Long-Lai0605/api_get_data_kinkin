import streamlit as st
import backend as be
import pandas as pd
import time

st.set_page_config(page_title="KINKIN ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# Khởi tạo DB
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

st.title("⚡ 1OFFICE PARALLEL ENGINE")

tab1, tab2 = st.tabs(["🚀 Dashboard", "➕ Thêm Khối Mới"])

# --- TAB 1: DASHBOARD ---
with tab1:
    blocks = be.get_active_blocks(st.secrets)
    
    if not blocks:
        st.info("Chưa có cấu hình nào.")
    else:
        df = pd.DataFrame(blocks)
        if 'Access Token (Encrypted)' in df.columns:
            df['Access Token (Encrypted)'] = "Đã lưu kho 🔒"

        # Đổi tên cột cho đẹp
        df.rename(columns={
            "Total Rows": "Dải dòng dữ liệu (Rows)",
            "Last Run": "Thực thi gần nhất"
        }, inplace=True)
            
        st.dataframe(df, use_container_width=True)
        
        if st.button("▶️ CHẠY TẤT CẢ", type="primary"):
            # Container để hiện log
            log_container = st.container()
            
            for i, block in enumerate(blocks):
                b_name = block['Block Name']
                if "Đã chốt" in block.get("Trạng thái", ""): continue

                # SỬ DỤNG ST.STATUS ĐỂ HIỂN THỊ QUÁ TRÌNH (Progress Steps)
                with st.status(f"🔄 **Đang xử lý khối: {b_name}**", expanded=True) as status:
                    
                    # 1. Gọi API (Song song)
                    status.write("📡 Đang kết nối API & Tải dữ liệu song song...")
                    
                    # Hàm callback để update status text từ bên trong backend (optional)
                    def update_status_text(text):
                        status.write(text)

                    token = block.get('Access Token (Encrypted)', '')
                    data, msg = be.fetch_1office_data_parallel(
                        block.get('API URL', ''), 
                        token, 
                        block.get('Method', 'GET'),
                        status_callback=update_status_text
                    )

                    if msg == "Success" and data:
                        status.write(f"✅ Đã tải xong {len(data)} dòng thô. Đang lọc dữ liệu...")
                        
                        # 2. Lọc dữ liệu Client-side
                        key_s = block.get("Filter Key Start", "")
                        d_s = block.get("Ngày bắt đầu", "")
                        key_e = block.get("Filter Key End", "")
                        d_e = block.get("Ngày kết thúc", "")
                        
                        # Chuyển string date về object date nếu có
                        date_s_obj = pd.to_datetime(d_s).date() if d_s else None
                        date_e_obj = pd.to_datetime(d_e).date() if d_e else None
                        
                        filtered_data = be.filter_data_client_side(data, key_s, date_s_obj, key_e, date_e_obj)
                        
                        status.write(f"🔍 Sau khi lọc: {len(filtered_data)} dòng. Đang ghi vào Sheet...")
                        
                        # 3. Ghi Sheet
                        range_str, w_msg = be.write_to_sheet_range(st.secrets, block, filtered_data)
                        
                        if "Error" not in w_msg:
                            status.update(label=f"✅ {b_name}: Hoàn thành! ({range_str})", state="complete", expanded=False)
                            st.toast(f"✅ {b_name}: +{len(filtered_data)} dòng ({range_str})")
                        else:
                            status.update(label=f"❌ {b_name}: Lỗi ghi sheet", state="error")
                            st.error(w_msg)
                            
                    elif msg == "Hết hạn API":
                        status.update(label=f"⛔ {b_name}: Token hết hạn!", state="error")
                    else:
                        status.update(label=f"⚠️ {b_name}: Lỗi API ({msg})", state="error")
            
            st.success("🎉 Đã chạy xong tất cả tiến trình!")
            time.sleep(2)
            st.rerun() # Refresh lại dashboard để cập nhật cột Last Run & Total Rows

# --- TAB 2: THÊM MỚI ---
with tab2:
    st.markdown("### Cấu hình Khối mới")
    with st.form("add_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên Khối (Block Name) *")
        method = c2.selectbox("Method", ["GET", "POST"])
        
        url = st.text_input("API URL *")
        token = st.text_input("Token *", type="password")
        
        c3, c4 = st.columns(2)
        link = c3.text_input("Link Sheet Đích *")
        sheet = c4.text_input("Tên Sheet Đích *")
        
        st.divider()
        st.markdown("**Cấu hình Lọc (Filter)**")
        
        # Nhóm Ngày bắt đầu
        col_s1, col_s2 = st.columns(2)
        start = col_s1.date_input("Ngày bắt đầu")
        key_start = col_s2.text_input("Trường so sánh lấy ngày bắt đầu", placeholder="VD: created_date")
        
        # Nhóm Ngày kết thúc
        col_e1, col_e2 = st.columns(2)
        end = col_e1.date_input("Ngày kết thúc")
        key_end = col_e2.text_input("Trường so sánh lấy ngày kết thúc", placeholder="VD: created_date")
        
        submitted = st.form_submit_button("Lưu & Cập nhật Dashboard")
        
        if submitted:
            if not name or not url or not token or not link:
                st.error("Thiếu thông tin bắt buộc!")
            else:
                ok = be.add_new_block(st.secrets, name, method, url, token, link, sheet, start, key_start, end, key_end)
                if ok:
                    st.toast("✅ Đã thêm thành công! Đang làm mới...")
                    time.sleep(1)
                    st.rerun() # TỰ ĐỘNG REFRESH TRANG
