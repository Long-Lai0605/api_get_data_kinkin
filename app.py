import streamlit as st
import utils
import pandas as pd

# 1. Cấu hình trang
st.set_page_config(page_title="Kinkin Automation", layout="wide")

# 2. Khởi tạo kết nối DB (Giữ nguyên logic cũ của bạn ở đầu app)
# Dòng này tương ứng với dòng 10 trong traceback cũ
try:
    sh = utils.init_db()
    if sh:
        st.sidebar.success("✅ Kết nối Database: OK")
except Exception as e:
    st.error("Không thể khởi tạo Database.")

# 3. Giao diện chính
st.title("Tool Get Data 1Office (Fixed)")

# Input Token
token_input = st.text_input("Nhập Access Token 1Office", type="password")

if st.button("Lấy dữ liệu"):
    if not token_input:
        st.warning("Vui lòng nhập Token!")
    else:
        with st.spinner("Đang gọi API 1Office..."):
            # Gọi hàm đã sửa lỗi trong utils
            data = utils.get_1office_data(token_input)
            
            if data:
                st.success(f"Thành công! Lấy được {len(data)} bản ghi.")
                
                # Hiển thị dữ liệu
                df = pd.DataFrame(data)
                st.dataframe(df)
                
                # Nút lưu (nếu cần dùng lại chức năng này)
                if st.button("Lưu vào Sheet"):
                    utils.save_to_sheet(data)
