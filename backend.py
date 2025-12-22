import gspread
import requests
import pandas as pd
import math
import time
import toml
import json
from datetime import datetime, timedelta
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

# --- HÀM DỰNG URL ---
def build_manual_url(base_url, access_token, limit, page, filters_list=None):
    params = {
        "access_token": access_token.strip(),
        "limit": limit,
        "page": page,
        "sort_by": "id",     
        "sort_type": "desc"
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

# --- HÀM FETCH THÔNG MINH (TAIL CHASER) ---
def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 50 # Mặc định 1Office
    
    filters_list = None
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        # Day+1 Strategy
        if date_end:
            server_end_date = date_end + timedelta(days=1)
            f_obj[f"{filter_key}_to"] = server_end_date.strftime("%d/%m/%Y")
        filters_list = [f_obj]

        if status_callback:
            status_callback(f"🎯 Filter: {json.dumps(filters_list)}")

    if status_callback: status_callback("📡 Gọi Page 1...")

    # --- BƯỚC 1: LẤY PAGE 1 ---
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
        
        if items: all_data.extend(items)
        if total_items == 0 and not items: return [], "Success (0 KQ)"

        # --- BƯỚC 2: TẢI SONG SONG ---
        estimated_pages = math.ceil(total_items / limit)
        
        if estimated_pages > 1:
            if status_callback: 
                status_callback(f"🚀 Tải song song {estimated_pages} trang (Total: {total_items})...")
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {}
                for p in range(2, estimated_pages + 1):
                    p_url = build_manual_url(url, token, limit, p, filters_list)
                    futures[executor.submit(fetch_single_page_manual, p_url, method)] = p
                    
                for future in as_completed(futures):
                    page_items = future.result()
                    if page_items:
                        all_data.extend(page_items)

        # --- BƯỚC 3: VÉT CẠN (TAIL CHASER) ---
        current_page = estimated_pages + 1
        max_safety_pages = 20 
        
        if status_callback: status_callback(f"🕵️ Đang rà soát thêm dữ liệu ẩn (Trang {current_page}+)...")
        
        empty_count = 0
        while empty_count < 1 and max_safety_pages > 0:
            p_url = build_manual_url(url, token, limit, current_page, filters_list)
            extra_items = fetch_single_page_manual(p_url, method)
            
            if extra_items and len(extra_items) > 0:
                all_data.extend(extra_items)
                if status_callback: status_callback(f"✅ Tìm thấy thêm {len(extra_items)} dòng ở trang {current_page}!")
                current_page += 1
                max_safety_pages -= 1
            else:
                empty_count += 1
        
        return all_data, "Success"
        
    except Exception as e:
        return None, str(e)

# --- [CẬP NHẬT] HÀM GHI SHEET (BÁO CÁO CHI TIẾT) ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        # Xóa cũ
        wks.clear()

        rows_to_write = []
        # Header
        first_item = data[0]
        api_headers = list(first_item.keys())
        system_headers = ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]
        rows_to_write.append(api_headers + system_headers)

        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        # Data
        for item in data:
            r = [item.get(k, "") for k in api_headers]
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_to_write.append(r)
            
        wks.update(values=rows_to_write, range_name='A1')
        
        # [MỚI] Tính toán dải dòng chính xác
        # Dòng 1 là Header, Dữ liệu bắt đầu từ dòng 2
        start_row = 2
        end_row = start_row + len(data) - 1
        range_str = f"Dòng {start_row} -> {end_row}"
        
        update_master_status(secrets_dict, b_name, range_str)
        return range_str, "Success"
    except Exception as e:
        return "0", f"Write Error: {e}"

# --- [CẬP NHẬT] HÀM UPDATE TRẠNG THÁI (GIỜ VN) ---
def update_master_status(secrets_dict, block_name, range_str):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("luu_cau_hinh")
        cell = wks.find(block_name)
        if cell:
            # [MỚI] Chuyển giờ UTC về giờ Việt Nam (UTC+7)
            utc_now = datetime.utcnow()
            vn_now = utc_now + timedelta(hours=7)
            time_str = vn_now.strftime("%H:%M %d/%m")
            
            wks.update_cell(cell.row, 8, time_str)
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
