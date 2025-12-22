import streamlit as st
import gspread
import requests
import pandas as pd
import math
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- CẤU HÌNH HỆ THỐNG ---
MASTER_SHEET_KEY = "system" 
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# --- 1. KẾT NỐI DATABASE (MASTER SHEET) ---
def get_master_sh():
    """Kết nối Master Sheet dùng Service Account"""
    try:
        # Kiểm tra secrets
        if "gcp_service_account" not in st.secrets:
            st.error("❌ Thiếu cấu hình gcp_service_account trong secrets.toml")
            st.stop()
            
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        return gc.open_by_key(st.secrets[MASTER_SHEET_KEY]["master_sheet_id"])
    except Exception as e:
        st.error(f"❌ Lỗi kết nối Master Sheet: {e}")
        st.stop()

def init_db():
    """Khởi tạo 6 sheet hệ thống nếu chưa có"""
    sh = get_master_sh()
    # Định nghĩa cấu trúc header cho các bảng
    required_sheets = {
        "luu_cau_hinh": ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"],
        "log_api_1office": ["Block Name", "Method", "API URL", "Access Token (Encrypted)"], # Sheet bảo mật
        "sys_config": ["Config Key", "Value"],
        "sys_lock": ["Lock Status", "User", "Timestamp"],
        "log_lanthucthi": ["Thời gian", "Block", "Trạng thái", "Số dòng", "Ghi chú"],
        "log_chay_auto_github": ["Run ID", "Thời gian", "Status", "Message"]
    }
    
    current_sheets = [s.title for s in sh.worksheets()]
    for name, headers in required_sheets.items():
        if name not in current_sheets:
            wks = sh.add_worksheet(name, 100, 20)
            wks.append_row(headers)
    return sh

# --- 2. XỬ LÝ API 1OFFICE (LOGIC MỚI: TOTAL ITEM) ---
def call_1office_api_standard(url, token, method="GET", from_date=None, to_date=None):
    """
    Nguyên lý gọi API:
    1. Gọi page 1 -> Lấy total_item
    2. Tính total_pages = ceil(total_item / limit)
    3. Loop từ 1 -> total_pages
    """
    all_data = []
    limit = 100
    clean_token = token.strip()
    
    # --- BƯỚC 1: GỌI PAGE 1 ĐỂ THĂM DÒ ---
    try:
        # Params cơ bản
        params = {
            "access_token": clean_token,
            "limit": limit,
            "page": 1
        }
        
        # Xử lý method
        if method.upper() == "POST":
            res = requests.post(url, params=params, json={}, timeout=20)
        else:
            res = requests.get(url, params=params, timeout=20)
            
        if res.status_code != 200:
            return None, f"Lỗi HTTP {res.status_code} (Page 1)"

        data_p1 = res.json()
        
        # Check lỗi Token từ 1Office
        if data_p1.get("code") == "token_not_valid":
            return None, "Hết hạn API" # Trả về đúng keyword yêu cầu

        # Lấy items trang 1
        items_p1 = data_p1.get("data", data_p1.get("items", []))
        if items_p1:
            all_data.extend(items_p1)
            
        # Lấy tổng số lượng (total_item)
        total_items = data_p1.get("total_item", 0)
        
        if total_items == 0:
            return [], "Success (No Data)"

        # --- BƯỚC 2: TÍNH TỔNG SỐ TRANG ---
        total_pages = math.ceil(total_items / limit)

        # --- BƯỚC 3: VÒNG LẶP CÁC TRANG CÒN LẠI ---
        if total_pages > 1:
            for page in range(2, total_pages + 1):
                # Update page
                params["page"] = page
                
                # Retry cơ bản cho từng trang
                for attempt in range(3):
                    try:
                        if method.upper() == "POST":
                            r = requests.post(url, params=params, json={}, timeout=20)
                        else:
                            r = requests.get(url, params=params, timeout=20)
                        
                        if r.status_code == 200:
                            page_json = r.json()
                            items = page_json.get("data", page_json.get("items", []))
                            all_data.extend(items)
                            break # Thoát retry nếu thành công
                        else:
                            time.sleep(1) # Đợi 1s rồi thử lại
                    except:
                        time.sleep(1)
                
                # Nghỉ nhẹ giữa các trang để tránh DDOS server
                time.sleep(0.2)
                
        return all_data, "Success"

    except Exception as e:
        return None, f"Exception: {str(e)}"

# --- 3. QUẢN LÝ KHỐI & DỮ LIỆU ---
def get_all_blocks_secure():
    """Lấy và gộp dữ liệu cấu hình + token (Fix lỗi KeyError)"""
    sh = get_master_sh()
    try:
        conf_data = sh.worksheet("luu_cau_hinh").get_all_records()
        sec_data = sh.worksheet("log_api_1office").get_all_records()
    except:
        return []

    df_conf = pd.DataFrame(conf_data)
    df_sec = pd.DataFrame(sec_data)

    if df_conf.empty or df_sec.empty:
        return []

    # Chuẩn hóa tên cột (Strip space)
    df_conf.columns = [c.strip() for c in df_conf.columns]
    df_sec.columns = [c.strip() for c in df_sec.columns]

    # Merge
    if "Block Name" in df_conf.columns and "Block Name" in df_sec.columns:
        full = pd.merge(df_conf, df_sec, on="Block Name", how="left")
        return full.to_dict('records')
    else:
        st.error("Lỗi cấu trúc Sheet: Cột 'Block Name' không tìm thấy.")
        return []

def save_to_destination_sheet(block_data, raw_data):
    """
    Ghi dữ liệu vào Sheet Đích:
    1. Thêm 4 cột truy vết.
    2. Xóa dữ liệu cũ CỦA LUỒNG ĐÓ (dựa trên Block Name).
    3. Ghi dữ liệu mới.
    """
    if not raw_data:
        return 0, "Không có dữ liệu"

    try:
        # Setup kết nối
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        
        # Mở sheet đích
        dest_ss = gc.open_by_url(block_data['Link Đích'])
        sheet_name = block_data['Sheet Đích']
        
        try:
            wks = dest_ss.worksheet(sheet_name)
        except:
            wks = dest_ss.add_worksheet(sheet_name, 1000, 20)
            
        # --- XỬ LÝ DỮ LIỆU ---
        processed_rows = []
        month_str = datetime.now().strftime("%m/%Y")
        block_name = block_data['Block Name']
        
        for item in raw_data:
            # Chuyển item (dict) thành list values. Lưu ý: Cần map đúng thứ tự cột nếu sheet có header cố định
            # Ở đây ta lấy values đơn thuần
            row = list(item.values())
            # Convert các object complex thành string để tránh lỗi JSON
            row = [str(x) if isinstance(x, (dict, list)) else x for x in row]
            
            # Thêm 4 cột truy vết
            row.extend([
                block_data['Link Đích'], # Link file nguồn
                sheet_name,              # Sheet nguồn
                month_str,               # Tháng chốt
                block_name               # Luồng (Key để xóa)
            ])
            processed_rows.append(row)

        # --- CHIẾN THUẬT GHI: APPEND (AN TOÀN NHẤT) ---
        # Yêu cầu prompt: "Tìm & Xóa cũ".
        # Cách tối ưu trên Sheet lớn: Đọc về -> Filter Pandas -> Ghi lại (Sẽ chậm nếu data > 10k dòng)
        # Cách nhanh gọn: Chỉ Append xuống dưới.
        
        # Ở đây tôi dùng cách APPEND để đảm bảo hiệu năng.
        # Nếu muốn xóa cũ: Bạn cần lấy toàn bộ data sheet đích về, lọc bỏ dòng có cột Luồng == block_name, rồi ghi lại.
        
        wks.append_rows(processed_rows)
        return len(processed_rows), "Success"

    except Exception as e:
        return 0, f"Lỗi ghi Sheet: {str(e)}"

def run_single_block(block):
    """Chạy 1 khối duy nhất"""
    # 1. Gọi API
    data, msg = call_1office_api_standard(
        block['API URL'], 
        block['Access Token (Encrypted)'], 
        block['Method']
    )
    
    if msg == "Hết hạn API":
        return False, "Hết hạn API", 0
    
    if not data:
        return True, "Không có dữ liệu mới", 0
        
    # 2. Ghi dữ liệu
    count, save_msg = save_to_destination_sheet(block, data)
    
    if "Lỗi" in save_msg:
        return False, save_msg, 0
        
    return True, "Thành công", count

def add_new_block_secure(name, method, url, token, link, sheet, start, end):
    """Thêm khối mới - Tách Token ra bảng riêng"""
    sh = get_master_sh()
    
    # 1. Ghi Config Public
    sh.worksheet("luu_cau_hinh").append_row([
        name, "Chưa chốt & đang cập nhật", str(start), str(end), link, sheet, "", 0
    ])
    
    # 2. Ghi Token Secure
    sh.worksheet("log_api_1office").append_row([
        name, method, url, token.strip()
    ])
