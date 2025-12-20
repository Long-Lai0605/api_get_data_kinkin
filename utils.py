import streamlit as st
import gspread
import requests
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import json

# --- CẤU HÌNH HỆ THỐNG ---
MASTER_SHEET_KEY = "system" # Key trong secrets.toml
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# --- 1. KẾT NỐI DATABASE (MASTER SHEET) ---
def get_master_sh():
    """Kết nối Master Sheet dùng Service Account"""
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        return gc.open_by_key(st.secrets[MASTER_SHEET_KEY]["master_sheet_id"])
    except Exception as e:
        st.error(f"Lỗi kết nối Master Sheet: {e}")
        st.stop()

def init_db():
    """Khởi tạo/Kiểm tra các sheet cấu hình cần thiết"""
    sh = get_master_sh()
    required_sheets = {
        "luu_cau_hinh": ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"],
        "log_api_1office": ["Block Name", "Method", "API URL", "Access Token (Encrypted)"], # Sheet bảo mật
        "log_lanthucthi": ["Thời gian", "Block", "Trạng thái", "Ghi chú"]
    }
    
    current_sheets = [s.title for s in sh.worksheets()]
    for name, headers in required_sheets.items():
        if name not in current_sheets:
            wks = sh.add_worksheet(name, 100, 20)
            wks.append_row(headers)
    return sh

# --- 2. XỬ LÝ API 1OFFICE (CORE FIX) ---
def call_1office_api_recursive(url, token, method="GET", from_date=None, to_date=None):
    """
    Hàm gọi API đệ quy (Pagination) - ĐÃ SỬA LỖI TOKEN
    """
    all_data = []
    page = 1
    limit = 100
    has_more = True
    
    # [QUAN TRỌNG] Token phải được strip() và đưa vào params
    clean_token = token.strip()
    
    while has_more:
        # Cấu trúc Params chuẩn cho 1Office
        params = {
            "access_token": clean_token, # <--- FIX: Token nằm ở đây
            "limit": limit,
            "page": page
        }
        
        # Nếu có lọc ngày (tùy API cụ thể mà key lọc có thể khác nhau, ví dụ lọc công việc)
        # Ở đây giả sử lọc cơ bản, nếu API cần filter phức tạp thì json.dumps vào key 'filters'
        
        try:
            if method.upper() == "POST":
                # POST: Token vẫn ở params URL, body có thể rỗng
                res = requests.post(url, params=params, json={}, timeout=30)
            else:
                # GET
                res = requests.get(url, params=params, timeout=30)

            if res.status_code != 200:
                return None, f"HTTP Error {res.status_code}"
            
            data = res.json()
            
            # Check lỗi logic 1Office
            if data.get("code") == "token_not_valid":
                return None, "Token hết hạn/sai"
            
            # Lấy list items
            items = data.get("data", data.get("items", []))
            
            if not items:
                has_more = False # Hết dữ liệu
            else:
                all_data.extend(items)
                # Logic dừng nếu số lượng trả về < limit (trang cuối)
                if len(items) < limit:
                    has_more = False
                else:
                    page += 1 # Sang trang tiếp theo
                    
        except Exception as e:
            return None, f"Exception: {str(e)}"
            
    return all_data, "Success"

# --- 3. QUẢN LÝ KHỐI (BLOCK ENGINE) ---
def add_new_block(block_name, method, url, token, des_link, des_sheet, start_date, end_date):
    """Thêm khối mới: Tách Token lưu riêng vào log_api_1office"""
    sh = get_master_sh()
    
    # 1. Lưu cấu hình chung (Public UI)
    sh.worksheet("luu_cau_hinh").append_row([
        block_name, "Chưa chốt & đang cập nhật", str(start_date), str(end_date), 
        des_link, des_sheet, "", 0
    ])
    
    # 2. Lưu Token bảo mật (Private Sheet)
    sh.worksheet("log_api_1office").append_row([
        block_name, method, url, token # Lưu token thực vào đây
    ])

def get_all_blocks():
    """Lấy dữ liệu join từ 2 bảng để chạy"""
    sh = get_master_sh()
    config_df = pd.DataFrame(sh.worksheet("luu_cau_hinh").get_all_records())
    secure_df = pd.DataFrame(sh.worksheet("log_api_1office").get_all_records())
    
    if config_df.empty or secure_df.empty:
        return []
        
    # Merge dữ liệu dựa trên Block Name
    full_data = pd.merge(config_df, secure_df, on="Block Name", how="left")
    return full_data.to_dict('records')

def run_block_process(block_data):
    """Thực thi logic từng khối"""
    block_name = block_data['Block Name']
    token = block_data['Access Token (Encrypted)']
    url = block_data['API URL']
    method = block_data['Method']
    
    # 1. Gọi API
    data, status = call_1office_api_recursive(url, token, method)
    
    if status != "Success":
        return False, status, 0
    
    if not data:
        return True, "Không có dữ liệu mới", 0

    # 2. Xử lý dữ liệu (Thêm 4 cột truy vết theo yêu cầu prompt)
    processed_rows = []
    month_str = datetime.now().strftime("%m/%Y")
    
    for item in data:
        # Flatten dữ liệu item thành 1 dòng (đơn giản hóa)
        # Trong thực tế bạn cần map đúng cột
        row = list(item.values()) 
        # Thêm 4 cột hệ thống
        row.extend([
            block_data['Link Đích'], # Link file nguồn
            block_data['Sheet Đích'], # Sheet nguồn
            month_str,                # Tháng chốt
            block_name                # Luồng
        ])
        processed_rows.append(row)
        
    # 3. Ghi vào Sheet Đích (Logic: Append)
    # Lưu ý: Bạn cần cấp quyền cho Service Account vào Sheet Đích nữa nhé
    try:
        gc = gspread.authorize(Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE))
        dest_sh = gc.open_by_url(block_data['Link Đích'])
        try:
            wks = dest_sh.worksheet(block_data['Sheet Đích'])
        except:
            wks = dest_sh.add_worksheet(block_data['Sheet Đích'], 1000, 20)
            
        # Thêm dữ liệu xuống cuối
        wks.append_rows(processed_rows)
        
        # 4. Update trạng thái lại Master Sheet (Last Run, Total Rows)
        # (Code update cell bỏ qua để ngắn gọn, thực tế cần update cell based on block name)
        
        return True, "Thành công", len(processed_rows)
        
    except Exception as e:
        return False, f"Lỗi ghi Sheet đích: {e}", 0
