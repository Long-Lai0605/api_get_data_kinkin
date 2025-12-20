import streamlit as st
import gspread
import requests
import pandas as pd
from google.oauth2.service_account import Credentials

# --- 1. KHU VỰC KẾT NỐI GOOGLE SHEETS (Đã sửa lỗi quyền truy cập) ---

def get_master_sh():
    """
    Hàm này lấy kết nối đến Google Sheet Master.
    Đã sửa: Thêm try/except để bắt lỗi nếu chưa Share quyền cho Service Account.
    """
    try:
        # Cấu hình scope đầy đủ (quan trọng để tránh lỗi 403)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Lấy thông tin credentials từ secrets
        if "gcp_service_account" not in st.secrets:
            st.error("Chưa cấu hình gcp_service_account trong secrets.toml")
            st.stop()
            
        credentials_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        gc = gspread.authorize(creds)

        # Mở sheet theo ID
        master_id = st.secrets["system"]["master_sheet_id"]
        return gc.open_by_key(master_id)

    except Exception as e:
        # In lỗi chi tiết ra để debug (thay vì bị Streamlit ẩn đi)
        st.error(f"❌ Lỗi kết nối Google Sheet: {e}")
        st.warning("👉 Kiểm tra: Bạn đã Share quyền Editor cho email Service Account trong file Sheet chưa?")
        st.stop()

def init_db():
    """
    Hàm khởi tạo database (như trong log cũ của bạn).
    Chức năng: Kiểm tra kết nối ngay khi vào app.
    """
    try:
        sh = get_master_sh()
        # Thử truy cập để chắc chắn kết nối thông suốt
        # Có thể thêm logic tạo sheet nếu chưa có ở đây
        return sh
    except Exception as e:
        st.error(f"Lỗi khởi tạo DB: {e}")
        return None

# --- 2. KHU VỰC GỌI API 1OFFICE (Đã sửa lỗi Token) ---

def get_1office_data(token):
    """
    Lấy dữ liệu nhân sự/công việc từ 1Office.
    Đã sửa: Token được truyền vào PARAMS để hiện lên URL (Khắc phục lỗi token_not_valid).
    """
    # URL API (Bạn có thể đổi sang api/work/process/gets nếu muốn lấy công việc)
    url = "https://kinkin.1office.vn/api/personnel/profile/gets"
    
    # [FIX QUAN TRỌNG]: Token nằm ở đây (Query Params)
    params = {
        "access_token": token.strip(), # Cắt khoảng trắng thừa do copy paste
        "limit": 100,
        "page": 1
    }

    try:
        # Gửi request POST với params (token sẽ lên URL)
        response = requests.post(url, params=params, json={})
        
        # Debug: In URL ra console hệ thống để kiểm tra
        print(f"Calling API: {response.url}")

        if response.status_code == 200:
            result = response.json()
            
            # Kiểm tra mã lỗi nghiệp vụ từ 1Office
            if result.get("code") == "token_not_valid":
                st.error("Token không hợp lệ hoặc đã hết hạn! Vui lòng lấy Token mới.")
                return None
            
            # Trả về danh sách data
            # API 1Office thường trả data ở key 'data' hoặc 'items'
            return result.get("data", result.get("items", []))
        else:
            st.error(f"Lỗi HTTP {response.status_code}")
            return None

    except Exception as e:
        st.error(f"Lỗi khi gọi API: {e}")
        return None

def save_to_sheet(data, sheet_name="Data_API"):
    """Lưu dữ liệu vào Sheet (Chức năng cũ)"""
    if not data:
        return
    
    sh = get_master_sh()
    try:
        wks = sh.worksheet(sheet_name)
    except:
        wks = sh.add_worksheet(sheet_name, 1000, 20)
    
    df = pd.DataFrame(data)
    wks.clear()
    wks.update([df.columns.values.tolist()] + df.values.tolist())
    st.success(f"Đã lưu {len(data)} dòng vào sheet '{sheet_name}'")
