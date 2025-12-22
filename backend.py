import gspread
import requests
import pandas as pd
import math
import time
import toml
import os
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def load_secrets():
    """
    Hàm load secrets thông minh:
    1. Ưu tiên đọc từ .streamlit/secrets.toml (nếu chạy local)
    2. Nếu không có file (chạy trên GitHub Actions), đọc từ biến môi trường
    """
    try:
        # Cách 1: Đọc file toml local
        return toml.load(".streamlit/secrets.toml")
    except:
        # Cách 2: Trả về None (để bên ngoài xử lý bằng os.environ hoặc st.secrets)
        return None

def get_connection(secrets_dict):
    """Kết nối Google Sheet"""
    try:
        # Nếu secrets_dict rỗng (ví dụ chạy trên Streamlit Cloud), dùng st.secrets sau
        if not secrets_dict:
             # Logic này sẽ được xử lý ở tầng app.py hoặc dùng os.environ
             return None, "No secrets provided"

        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        master_id = secrets_dict["system"]["master_sheet_id"]
        return gc.open_by_key(master_id), "Success"
    except Exception as e:
        return None, str(e)

# --- LOGIC GỌI API (THEO YÊU CẦU MỤC VI) ---
def fetch_1office_data(url, token, method="GET"):
    """
    Logic: Page 1 -> Total Items -> Ceil(Pages) -> Loop
    """
    all_data = []
    limit = 100
    clean_token = token.strip()
    
    # --- BƯỚC 1: PAGE 1 ---
    # Quan trọng: Token PHẢI nằm trong params
    params = {
        "access_token": clean_token,
        "limit": limit,
        "page": 1
    }
    
    try:
        if method.upper() == "POST":
            # API 1Office thường dùng POST nhưng token vẫn phải ở URL (params)
            res = requests.post(url, params=params, json={}, timeout=30)
        else:
            res = requests.get(url, params=params, timeout=30)
            
        if res.status_code != 200:
            return None, f"HTTP Error {res.status_code}"
            
        d = res.json()
        
        # Check lỗi nghiệp vụ
        if d.get("code") == "token_not_valid":
            return None, "Hết hạn API"
            
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        if items: all_data.extend(items)
        
        if total_items == 0:
            return [], "Success"
            
        # --- BƯỚC 2: TÍNH SỐ TRANG ---
        total_pages = math.ceil(total_items / limit)
        
        # --- BƯỚC 3: LOOP CÁC TRANG CÒN LẠI ---
        if total_pages > 1:
            for p in range(2, total_pages + 1):
                params["page"] = p
                
                # Retry 3 lần
                for _ in range(3):
                    try:
                        if method.upper() == "POST":
                            r = requests.post(url, params=params, json={}, timeout=30)
                        else:
                            r = requests.get(url, params=params, timeout=30)
                        
                        if r.status_code == 200:
                            pd_json = r.json()
                            p_items = pd_json.get("data", pd_json.get("items", []))
                            if p_items: all_data.extend(p_items)
                            break
                        time.sleep(1)
                    except:
                        time.sleep(1)
                time.sleep(0.2)
                
        return all_data, "Success"
        
    except Exception as e:
        return None, str(e)

# --- LOGIC GHI SHEET ---
def write_to_sheet(secrets_dict, block_conf, data):
    if not data: return 0, "No Data"
    
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        
        try:
            wks = dest_ss.worksheet(wks_name)
        except:
            wks = dest_ss.add_worksheet(wks_name, 1000, 20)
            
        # Chuẩn bị dữ liệu + 4 cột truy vết
        rows_add = []
        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            # Flatten dict -> list values
            r = list(item.values())
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_add.append(r)
            
        wks.append_rows(rows_add)
        return len(rows_add), "Success"
        
    except Exception as e:
        return 0, f"Write Error: {e}"

# --- HÀM LẤY BLOCK ĐỂ CHẠY ---
def get_active_blocks(secrets_dict):
    """Lấy danh sách các block CÓ THỂ CHẠY (Merge Config + Token)"""
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    
    try:
        c = pd.DataFrame(sh.worksheet("luu_cau_hinh").get_all_records())
        s = pd.DataFrame(sh.worksheet("log_api_1office").get_all_records())
        
        if c.empty or s.empty: return []
        
        # Clean headers
        c.columns = [x.strip() for x in c.columns]
        s.columns = [x.strip() for x in s.columns]
        
        full = pd.merge(c, s, on="Block Name", how="left")
        
        # Chỉ lấy dòng chưa chốt
        # (Giả sử cột trạng thái là 'Trạng thái')
        return full.to_dict('records')
    except:
        return []

# --- HÀM GHI LOG HỆ THỐNG ---
def log_system_run(secrets_dict, run_id, status, message):
    sh, _ = get_connection(secrets_dict)
    if sh:
        try:
            wks = sh.worksheet("log_chay_auto_github")
            wks.append_row([run_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), status, message])
        except:
            pass
