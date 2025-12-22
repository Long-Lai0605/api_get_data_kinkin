import gspread
import requests
import pandas as pd
import math
import time
import toml
from datetime import datetime
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def load_secrets_headless():
    try: return toml.load(".streamlit/secrets.toml")
    except: return None

def get_connection(secrets_dict):
    try:
        if not secrets_dict: return None, "Secrets is empty"
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        master_id = secrets_dict["system"]["master_sheet_id"]
        return gc.open_by_key(master_id), "Success"
    except Exception as e: return None, str(e)

def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    # [CẬP NHẬT SCHEMA] Gộp key start/end thành 1 cột "Filter Key"
    schemas = {
        "luu_cau_hinh": ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Filter Key", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"],
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

# --- LOGIC LỌC NGÀY (UPDATED: 1 KEY DUY NHẤT) ---
def parse_date(date_str):
    """Chuyển đổi chuỗi ngày từ 1Office về datetime"""
    if not date_str: return None
    # Các định dạng ngày thường gặp của 1Office
    formats = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]
    for fmt in formats:
        try:
            # Chỉ lấy phần ngày, bỏ phần giờ nếu có
            clean_str = str(date_str).split(' ')[0]
            return datetime.strptime(clean_str, fmt)
        except: continue
    return None

def filter_data_client_side(data, filter_key, date_start, date_end):
    """
    Lọc dữ liệu dựa trên 1 trường duy nhất (filter_key).
    So sánh trường đó với date_start và date_end.
    """
    if not data: return []
    
    # Nếu không có key lọc, trả về toàn bộ
    if not filter_key:
        return data

    filtered = []
    # Chuyển đổi ngày cấu hình (là object date của python) sang datetime để so sánh
    d_start = datetime.combine(date_start, datetime.min.time()) if date_start else None
    d_end = datetime.combine(date_end, datetime.max.time()) if date_end else None

    for item in data:
        is_valid = True
        val_str = item.get(filter_key) # Lấy giá trị của trường cần lọc (VD: created_date)
        val_date = parse_date(val_str)

        if not val_date:
            # Nếu bản ghi này không có ngày, hoặc format sai -> Tùy chọn: Bỏ qua hoặc Giữ lại?
            # Ở đây ta chọn BỎ QUA để an toàn.
            continue 

        # 1. Kiểm tra ngày bắt đầu (>=)
        if d_start and val_date < d_start:
            is_valid = False
        
        # 2. Kiểm tra ngày kết thúc (<=)
        if is_valid and d_end and val_date > d_end:
            is_valid = False
                
        if is_valid:
            filtered.append(item)
            
    return filtered

# --- FETCH API PARALLEL ---
def fetch_single_page(url, params, method, page_num):
    p = params.copy()
    p["page"] = page_num
    try:
        if method.upper() == "POST":
            r = requests.post(url, params=p, json={}, timeout=30)
        else:
            r = requests.get(url, params=p, timeout=30)
        if r.status_code == 200:
            d = r.json()
            return d.get("data", d.get("items", []))
    except: pass
    return []

def fetch_1office_data_parallel(url, token, method="GET", status_callback=None):
    all_data = []
    limit = 100
    clean_token = str(token).strip()
    params = {"access_token": clean_token, "limit": limit}

    if status_callback: status_callback("📡 Đang gọi Page 1 để lấy tổng số...")

    try:
        if method.upper() == "POST":
            res = requests.post(url, params={**params, "page": 1}, json={}, timeout=30)
        else:
            res = requests.get(url, params={**params, "page": 1}, timeout=30)
            
        if res.status_code != 200: return None, f"HTTP {res.status_code}"
        d = res.json()
        if d.get("code") == "token_not_valid": return None, "Hết hạn API"
        
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        if items: all_data.extend(items)
        
        if total_items == 0: return [], "Success"

        total_pages = math.ceil(total_items / limit)
        
        if total_pages > 1:
            if status_callback: status_callback(f"🚀 Tìm thấy {total_pages} trang. Đang tải song song...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_single_page, url, params, method, p): p for p in range(2, total_pages + 1)}
                for future in as_completed(futures):
                    page_items = future.result()
                    if page_items: all_data.extend(page_items)
                    
        return all_data, "Success"
    except Exception as e:
        return None, str(e)

# --- GHI SHEET ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        last_row_start = len(wks.get_all_values()) + 1 
        rows_add = []
        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            r = list(item.values())
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_add.append(r)
            
        wks.append_rows(rows_add)
        last_row_end = last_row_start + len(rows_add) - 1
        range_str = f"Dòng {last_row_start} -> {last_row_end}"
        update_master_status(secrets_dict, b_name, range_str)
        return range_str, "Success"
    except Exception as e:
        return "0", f"Write Error: {e}"

def update_master_status(secrets_dict, block_name, range_str):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("luu_cau_hinh")
        cell = wks.find(block_name)
        if cell:
            # Update Last Run (Cột 8), Total Rows (Cột 9) theo schema mới
            # Schema: Name(1), Status(2), Start(3), End(4), Key(5), Link(6), Sheet(7), LastRun(8), Total(9)
            now = datetime.now().strftime("%H:%M %d/%m")
            wks.update_cell(cell.row, 8, now)
            wks.update_cell(cell.row, 9, range_str)
    except: pass

def get_active_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    try:
        c = pd.DataFrame(sh.worksheet("luu_cau_hinh").get_all_records())
        s = pd.DataFrame(sh.worksheet("log_api_1office").get_all_records())
        if c.empty or s.empty: return []
        
        c.columns = [x.strip() for x in c.columns]
        s.columns = [x.strip() for x in s.columns]
        
        # [CẬP NHẬT] Kiểm tra cột "Filter Key"
        if "Filter Key" not in c.columns: c["Filter Key"] = ""

        full = pd.merge(c, s, on="Block Name", how="left")
        
        display_cols = ["Block Name", "Trạng thái", "Method", "API URL", "Access Token (Encrypted)", 
                        "Link Đích", "Sheet Đích", "Ngày bắt đầu", "Ngày kết thúc", "Filter Key",
                        "Total Rows", "Last Run"]
        
        final_cols = [col for col in display_cols if col in full.columns]
        return full[final_cols].fillna("").to_dict('records')
    except: return []

def add_new_block(secrets_dict, name, method, url, token, link, sheet, start, end, filter_key):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    # [CẬP NHẬT] Ghi 1 Key duy nhất
    sh.worksheet("luu_cau_hinh").append_row([
        name, "Chưa chốt & đang cập nhật", str(start), str(end), filter_key, link, sheet, "", ""
    ])
    sh.worksheet("log_api_1office").append_row([name, method, url, token.strip()])
    return True
