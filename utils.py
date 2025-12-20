import streamlit as st
import gspread
import requests
import pandas as pd
from google.oauth2.service_account import Credentials

# --- PHẦN 1: KẾT NỐI GOOGLE SHEETS ---
def get_master_sh():
    """Kết nối đến Google Sheet Master"""
    try:
        # Định nghĩa scope để quyền truy cập đầy đủ
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Lấy credentials từ secrets
        credentials_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        gc = gspread.authorize(creds)

        # Mở sheet bằng ID từ secrets
        sheet_id = st.secrets["system"]["master_sheet_id"]
        return gc.open_by_key(sheet_id)

    except Exception as e:
        st.error("❌ Lỗi kết nối Google Sheet!")
        st.error(f"Chi tiết: {e}")
        st.info("💡 Gợi ý: Hãy kiểm tra xem bạn đã Share quyền Editor cho email Service Account chưa?")
        st.stop() # Dừng chương trình để người dùng sửa lỗi

# --- PHẦN 2: GỌI API 1OFFICE (ĐÃ SỬA LỖI TOKEN) ---
def get_1office_data(token):
    """
    Lấy dữ liệu từ 1Office với Token được truyền đúng vào URL Params
    """
    # URL API (Theo ảnh bạn gửi là API nhân sự)
    url = "https://kinkin.1office.vn/api/personnel/profile/gets"
    
    # QUAN TRỌNG: Token phải nằm ở đây để hiện lên URL (Query String)
    # Tham khảo logic từ file mẫu dòng 40
    params = {
        "access_token": token.strip(), # Cắt khoảng trắng thừa
        "limit": 100,
        "page": 1
        # Nếu muốn filter thì thêm key "filters" ở đây
    }

    try:
        # Gửi request POST (Theo ảnh bạn gửi method là POST)
        response = requests.post(url, params=params, json={})
        
        # Debug: In ra URL để kiểm tra (chỉ hiện ở terminal)
        print(f"URL Request: {response.url}")

        if response.status_code == 200:
            data = response.json()
            
            # Kiểm tra lỗi logic từ 1Office trả về (ví dụ token sai)
            if data.get("code") == "token_not_valid":
                return {"error": "Token không hợp lệ hoặc đã hết hạn!"}
            
            # Trả về danh sách dữ liệu (items hoặc data)
            # Logic lấy items tương tự dòng 44-45 file mẫu
            return data.get("data", data.get("items", []))
        else:
            return {"error": f"Lỗi HTTP: {response.status_code}"}
            
    except Exception as e:
        return {"error": f"Lỗi ngoại lệ: {str(e)}"}

# --- PHẦN 3: LƯU DATA VÀO SHEET (TÙY CHỌN) ---
def save_data_to_sheet(data_list, sheet_name="Data_Moi"):
    """Ghi dữ liệu danh sách dictionary vào Google Sheet"""
    if not data_list:
        return
    
    sh = get_master_sh()
    
    # Tìm hoặc tạo worksheet
    try:
        wks = sh.worksheet(sheet_name)
    except:
        wks = sh.add_worksheet(title=sheet_name, rows=1000, cols=20)
        
    # Chuyển đổi list of dicts thành DataFrame để dễ xử lý
    df = pd.DataFrame(data_list)
    
    # Ghi header và dữ liệu
    wks.clear() # Xóa cũ
    wks.update([df.columns.values.tolist()] + df.values.tolist())
    return True
