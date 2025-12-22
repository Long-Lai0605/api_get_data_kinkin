import gspread
import requests
import pandas as pd
import math
import time
import toml
import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote

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

# --- HÀM XỬ LÝ NGÀY ---
def parse_date_val(date_str):
    if not date_str: return None
    s = str(date_str).strip()
    formats = [
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", 
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"
    ]
    for fmt in formats:
        try: return datetime.strptime(s, fmt)
        except: continue
    try: return datetime.strptime(s.split(' ')[0], "%d/%m/%Y")
    except: pass
    return None

def filter_chunk_client_side(items, filter_key, date_start, date_end):
    if not filter_key or (not date_start and not date_end): return items
    filtered = []
    d_start = datetime.combine(date_start, datetime.min.time()) if date_start else None
    d_end = datetime.combine(date_end, datetime.max.time()) if date_end else None
    for item in items:
        val_str = item.get(filter_key)
        if not val_str: continue 
        val_date = parse_date_val(val_str)
        # Nếu parse lỗi -> GIỮ LẠI (An toàn)
        if not val_date: 
            filtered.append(item)
            continue
        if d_start and val_date < d_start: continue
        if d_end and val_date > d_end: continue
        filtered.append(item)
    return filtered

# --- HÀM DỰNG URL CHUẨN ---
def build_manual_url(base_url, access_token, limit, page, filters_list=None):
    # [FIX] Thêm sort để đảm bảo lấy mới nhất trước
    params = {
        "access_token": access_token.strip(),
        "limit": limit,
        "page": page,
        "sort_by": "id",     # Sắp xếp theo ID
        "sort_type": "desc"  # Giảm dần (Mới nhất lên đầu)
    }
    query_string = urlencode(params)
    
    filter_part = ""
    if filters_list:
        json_str = json.dumps(filters_list, separators=(',', ':'))
        encoded_json = quote(json_str)
        filter_part = f"&filters={encoded_json}"
        
    return f"{base_url}?{query_string}{filter_part}"

def fetch_single_page_manual(full_url, method):
    try:
        if method.upper() == "POST":
            r = requests.post(full_url, json={}, timeout=30)
        else:
            r = requests.get(full_url, timeout=30)
        if r.status_code == 200:
            d = r.json()
            return d.get("data", d.get("items", []))
    except: pass
    return []

# --- HÀM FETCH THÔNG MINH (FIX LIMIT) ---
def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100 # Chúng ta mong muốn 100
    
    # Chuẩn bị bộ lọc
    filters_list = None
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: f_obj[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        filters_list = [f_obj] # Array

    if status_callback: status_callback("📡 Gọi Page 1...")

    # Page 1
    page1_url = build_manual_url(url, token, limit, 1, filters_list)
    
    try:
        if method.upper() == "POST":
            res = requests.post(page1_url, json={}, timeout=30)
        else:
            res = requests.get(page1_url, timeout=30)
            
        if res.status_code != 200: return None, f"HTTP {res.status_code}"
        d = res.json()
        if d.get("code") == "token_not_valid": return None, "Hết hạn API"
        
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        
        # [QUAN TRỌNG] Tự động phát hiện giới hạn thực tế của Server
        real_limit = limit
        if items:
            count_page_1 = len(items)
            # Nếu Page 1 trả về ít hơn limit yêu cầu (và total còn nhiều) -> Server đang ép limit
            if count_page_1 < limit and total_items > count_page_1:
                real_limit = count_page_1
                if status_callback: 
                    status_callback(f"⚠️ Phát hiện Server ép Limit: {real_limit} dòng/trang (Thay vì {limit})")
            
            # Lọc & Lưu Page 1
            clean_items = filter_chunk_client_side(items, filter_key, date_start, date_end)
            all_data.extend(clean_items)
        
        if total_items == 0: return [], "Success (0 KQ)"

        # Tính lại tổng số trang dựa trên real_limit
        total_pages = math.ceil(total_items / real_limit)
        
        if total_pages > 1:
            if status_callback: 
                status_callback(f"🚀 Tải song song {total_pages} trang (Total: {total_items})...")
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {}
                for p in range(2, total_pages + 1):
                    p_url = build_manual_url(url, token, limit, p, filters_list)
                    futures[executor.submit(fetch_single_page_manual, p_url, method)] = p
                    
                for future in as_completed(futures):
                    page_items = future.result()
                    if page_items:
                        clean_chunk = filter_chunk_client_side(page_items, filter_key, date_start, date_end)
                        all_data.extend(clean_chunk)
                    
        return all_data, "Success"
        
    except Exception as e:
        return None, str(e)

# --- HÀM GHI SHEET ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        wks.clear()

        rows_to_write = []
        first_item = data[0]
        api_headers = list(first_item.keys())
        system_headers = ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]
        rows_to_write.append(api_headers + system_headers)

        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            r = [item.get(k, "") for k in api_headers]
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_to_write.append(r)
            
        wks.update(values=rows_to_write, range_name='A1')
        range_str = f"Làm mới {len(data)} dòng"
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
    sh.worksheet("luu_cau_hinh").append_row([
        name, "Chưa chốt & đang cập nhật", str(start), str(end), filter_key, link, sheet, "", ""
    ])
    sh.worksheet("log_api_1office").append_row([name, method, url, token.strip()])
    return True
