import gspread
import requests
import pandas as pd
import math
import time
import toml
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def load_secrets_headless():
    """Dùng cho run_headless.py: Đọc file secrets.toml local"""
    try:
        return toml.load(".streamlit/secrets.toml")
    except:
        return None

def get_connection(secrets_dict):
    """Kết nối Google Sheet từ dict secrets"""
    try:
        if not secrets_dict:
             return None, "Secrets is empty"

        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        master_id = secrets_dict["system"]["master_sheet_id"]
        return gc.open_by_key(master_id), "Success"
    except Exception as e:
        return None, str(e)

def init_database(secrets_dict):
    """Khởi tạo cấu trúc bảng nếu chưa có"""
    sh, msg = get_connection(secrets_dict)
    if not sh: return

    schemas = {
        "luu_cau_hinh": ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"],
        "log_api_1office": ["Block Name", "Method", "API URL", "Access Token (Encrypted)"],
        "log_chay_auto_github": ["Run ID", "Thời gian", "Status", "Message"]
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass

# --- LOGIC GỌI API 1OFFICE (FIX LỖI TOKEN & PHÂN TRANG) ---
def fetch_1office_data(url, token, method="GET"):
    all_data = []
    limit = 100
    clean_token = token.strip()
    
    # [QUAN TRỌNG] Token phải nằm trong params để lên URL
    params = {
        "access_token": clean_token,
        "limit": limit,
        "page": 1
    }
    
    try:
        # --- BƯỚC 1: LẤY TRANG 1 & TOTAL ITEM ---
        if method.upper() == "POST":
            res = requests.post(url, params=params, json={}, timeout=30)
        else:
            res = requests.get(url, params=params, timeout=30)
            
        if res.status_code != 200:
            return None, f"HTTP Error {res.status_code}"
            
        d = res.json()
        
        # Check lỗi nghiệp vụ 1Office
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
                
                # Retry cơ bản (3 lần)
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
                time.sleep(0.2) # Delay tránh spam
                
        return all_data, "Success"
        
    except Exception as e:
        return None, str(e)

# --- GHI SHEET ---
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
            
        # Chuẩn bị data ghi
        rows_add = []
        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            r = list(item.values())
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            # Thêm 4 cột truy vết
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_add.append(r)
            
        wks.append_rows(rows_add)
        return len(rows_add), "Success"
        
    except Exception as e:
        return 0, f"Write Error: {e}"

# --- LẤY BLOCK ĐỂ CHẠY ---
def get_active_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    
    try:
        c = pd.DataFrame(sh.worksheet("luu_cau_hinh").get_all_records())
        s = pd.DataFrame(sh.worksheet("log_api_1office").get_all_records())
        
        if c.empty or s.empty: return []
        
        c.columns = [x.strip() for x in c.columns]
        s.columns = [x.strip() for x in s.columns]
        
        full = pd.merge(c, s, on="Block Name", how="left")
        return full.to_dict('records')
    except:
        return []

def add_new_block(secrets_dict, name, method, url, token, link, sheet, start, end):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    sh.worksheet("luu_cau_hinh").append_row([name, "Chưa chốt & đang cập nhật", str(start), str(end), link, sheet, "", 0])
    sh.worksheet("log_api_1office").append_row([name, method, url, token.strip()])
    return True
