import streamlit as st
import utils
import pandas as pd

# Cấu hình trang
st.set_page_config(page_title="Kinkin 1Office Tool", layout="wide")

st.title("🚀 Tool lấy dữ liệu 1Office")

# 1. Khu vực nhập liệu
with st.sidebar:
    st.header("Cấu hình")
    # Password field để ẩn token dài ngoằng
    token_input = st.text_input("Nhập Access Token 1Office", type="password")
    
    btn_get_data = st.button("Lấy dữ liệu ngay", type="primary")

# 2. Xử lý chính khi bấm nút
if btn_get_data:
    if not token_input:
        st.warning("⚠️ Vui lòng nhập Token trước!")
    else:
        with st.spinner("⏳ Đang kết nối API 1Office..."):
            # Gọi hàm bên utils
            raw_data = utils.get_1office_data(token_input)

        # 3. Kiểm tra kết quả trả về
        if isinstance(raw_data, dict) and "error" in raw_data:
            # Nếu có lỗi
            st.error(f"❌ Thất bại: {raw_data['error']}")
        
        elif isinstance(raw_data, list) and len(raw_data) > 0:
            # Nếu thành công và có dữ liệu
            st.success(f"✅ Thành công! Lấy được {len(raw_data)} bản ghi.")
            
            # Hiển thị bảng dữ liệu
            df = pd.DataFrame(raw_data)
            st.dataframe(df)

            # (Tùy chọn) Nút lưu vào Google Sheet
            if st.button("Lưu vào Google Sheet"):
                with st.spinner("Đang ghi vào Sheet..."):
                    utils.save_data_to_sheet(raw_data)
                    st.toast("Đã lưu dữ liệu thành công!", icon="🎉")
        
        else:
            st.info("API trả về thành công nhưng không có dữ liệu nào (Danh sách rỗng).")

# Hướng dẫn phụ
with st.expander("ℹ️ Hướng dẫn lấy Token"):
    st.write("""
    1. Đăng nhập 1Office.
    2. Nhấn F12 mở Developer Tools.
    3. Vào tab Network -> Thực hiện một hành động bất kỳ.
    4. Tìm request API -> Copy `access_token` trong phần Payload hoặc URL.
    """)
