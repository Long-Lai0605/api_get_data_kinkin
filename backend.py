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

# --- CÁC HÀM XỬ LÝ API GIỮ NGUYÊN ---
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

def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    clean_token = str(token).strip()
    
    params = {"access_token": clean_token, "limit": limit}

    # Lọc Server-side
    if filter_key and (date_start or date_end):
        filters_dict = {}
        if date_start: filters_dict[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: filters_dict[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        params["filters"] = json.dumps(filters_dict)
        if status_callback: status_callback(f"🎯 Kích hoạt lọc Server: {filters_dict}")

    if status_callback: status_callback("📡 Gọi Page 1 kiểm tra...")

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
        
        if total_items == 0: return [], "Success (0 KQ)"

        total_pages = math.ceil(total_items / limit)
        
        if total_pages > 1:
            if status_callback: status_callback(f"🚀 Tải song song {total_pages} trang...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_single_page, url, params, method, p): p for p in range(2, total_pages + 1)}
                for future in as_completed(futures):
                    page_items = future.result()
                    if page_items: all_data.extend(page_items)
                    
        return all_data, "Success"
    except Exception as e:
        return None, str(e)

# --- [QUAN TRỌNG] GHI SHEET CÓ HEADER TỰ ĐỘNG ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        # 1. Kiểm tra sheet có dữ liệu chưa
        # Lấy toàn bộ giá trị để check
        existing_values = wks.get_all_values()
        is_empty_sheet = len(existing_values) == 0
        
        rows_to_write = []
        
        # 2. NẾU SHEET TRỐNG -> TẠO HEADER (DÒNG 1)
        if is_empty_sheet:
            # Lấy keys từ bản ghi đầu tiên làm header
            first_item = data[0]
            api_headers = list(first_item.keys())
            
            # Thêm 4 header hệ thống vào sau
            system_headers = ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]
            full_header = api_headers + system_headers
            
            # Thêm dòng header vào danh sách cần ghi
            rows_to_write.append(full_header)

        # 3. CHUẨN BỊ DỮ LIỆU
        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            # Lấy values theo đúng thứ tự keys (để khớp với header)
            # Lưu ý: Python 3.7+ dict giữ order, nhưng để chắc chắn ta dùng keys của item đầu tiên
            if is_empty_sheet:
                # Nếu vừa tạo header, phải đảm bảo data khớp order với header đó
                r = [item.get(k, "") for k in api_headers] 
            else:
                # Nếu append vào sheet cũ, ta dùng values() và hy vọng cấu trúc không đổi
                # (Cách tốt nhất là đọc header cũ để map, nhưng append đơn giản dùng values)
                r = list(item.values())

            # Convert các object con thành string
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            
            # Thêm 4 cột hệ thống
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_to_write.append(r)
            
        # 4. GHI MỘT LẦN (BATCH UPDATE)
        wks.append_rows(rows_to_write)
        
        # 5. TÍNH TOÁN DÒNG ĐỂ BÁO CÁO
        start_row = len(existing_values) + 1
        end_row = start_row + len(rows_to_write) - 1
        range_str = f"Dòng {start_row} -> {end_row}"
        
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
