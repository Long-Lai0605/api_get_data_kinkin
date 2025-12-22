import streamlit as st
import backend as be  # Gọi backend là 'be' cho gọn
import pandas as pd
import time

st.set_page_config(page_title="KINKIN ENGINE", layout="wide", page_icon="🛡️")

# CSS
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

# Khởi tạo DB
with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

st.title("🛡️ 1OFFICE MULTI-BLOCK ENGINE")

# Tabs
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
        
        cols = ["Block Name", "Trạng thái", "Method", "API URL", "Access Token (Encrypted)", "Total Rows", "Last Run"]
        valid_cols = [c for c in cols if c in df.columns]
        st.dataframe(df[valid_cols], use_container_width=True)
        
        if st.button("▶️ CHẠY TẤT CẢ", type="primary"):
            progress = st.progress(0)
            status_box = st.empty()
            total = len(blocks)
            success = 0
            
            for i, block in enumerate(blocks):
                if "Đã chốt" in block.get("Trạng thái", ""): continue
                
                b_name = block['Block Name']
                status_box.text(f"Đang chạy: {b_name}...")
                
                # Gọi Backend
                data, msg = be.fetch_1office_data(block['API URL'], block['Access Token (Encrypted)'], block['Method'])
                
                if msg == "Success" and data:
                    count, w_msg = be.write_to_sheet(st.secrets, block, data)
                    if count > 0:
                        st.toast(f"✅ {b_name}: +{count} dòng")
                        success += 1
                    else:
                        st.error(f"{b_name}: Lỗi ghi ({w_msg})")
                elif msg == "Hết hạn API":
                    st.error(f"⛔ {b_name}: Token hết hạn!")
                else:
                    st.warning(f"⚠️ {b_name}: {msg}")
                
                progress.progress((i + 1) / total)
            
            status_box.success(f"Hoàn tất! Thành công: {success}")

# --- TAB 2: THÊM MỚI ---
with tab2:
    with st.form("add_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên Khối")
        method = c2.selectbox("Method", ["GET", "POST"])
        url = st.text_input("API URL")
        token = st.text_input("Token", type="password")
        c3, c4 = st.columns(2)
        link = c3.text_input("Link Sheet Đích")
        sheet = c4.text_input("Tên Sheet Đích")
        c5, c6 = st.columns(2)
        start = c5.date_input("Ngày bắt đầu")
        end = c6.date_input("Ngày kết thúc")
        
        if st.form_submit_button("Lưu Cấu Hình"):
            if not name or not url or not token:
                st.error("Thiếu thông tin bắt buộc!")
            else:
                ok = be.add_new_block(st.secrets, name, method, url, token, link, sheet, start, end)
                if ok: st.success("Đã thêm thành công!")
