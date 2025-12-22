import gspread
import requests
import pandas as pd
import math
import time
import toml
import json
import uuid
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

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
    
    # Cấu trúc DB mới: 2 Bảng (Blocks & Links)
    schemas = {
        "manager_blocks": ["Block ID", "Block Name", "Schedule Type", "Schedule Config", "Status", "Last Run"],
        "manager_links": ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"],
        "log_system": ["Time", "Block", "Message", "Type"]
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass

# --- CÁC HÀM CRUD (QUẢN LÝ KHỐI & LINK) ---

def create_block(secrets_dict, block_name, schedule_type="Thủ công", schedule_config="{}"):
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_blocks")
    block_id = str(uuid.uuid4())[:8]
    wks.append_row([block_id, block_name, schedule_type, schedule_config, "Active", ""])
    return True

def delete_block(secrets_dict, block_id):
    """Xóa khối và toàn bộ link con của nó"""
    sh, _ = get_connection(secrets_dict)
    
    # 1. Xóa trong manager_blocks
    wks_b = sh.worksheet("manager_blocks")
    cells = wks_b.findall(block_id)
    # Xóa từ dưới lên để không lệch index
    rows_to_delete = sorted([c.row for c in cells], reverse=True)
    for r in rows_to_delete: wks_b.delete_rows(r)
    
    # 2. Xóa trong manager_links
    wks_l = sh.worksheet("manager_links")
    cells_l = wks_l.findall(block_id)
    rows_l_to_delete = sorted([c.row for c in cells_l], reverse=True)
    for r in rows_l_to_delete: wks_l.delete_rows(r)
    return True

def add_link_to_block(secrets_dict, block_id, method, url, token, link_sheet, sheet_name, f_key, d_start, d_end):
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_links")
    link_id = str(uuid.uuid4())[:8]
    
    # Format ngày
    s_str = d_start.strftime("%Y-%m-%d") if d_start else ""
    e_str = d_end.strftime("%Y-%m-%d") if d_end else ""
    
    wks.append_row([link_id, block_id, method, url, token, link_sheet, sheet_name, f_key, s_str, e_str, "Active"])
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    return sh.worksheet("manager_blocks").get_all_records()

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    all_links = sh.worksheet("manager_links").get_all_records()
    # Lọc theo Block ID
    return [l for l in all_links if str(l.get("Block ID")) == str(block_id)]

def update_block_config(secrets_dict, block_id, schedule_type, schedule_config):
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_blocks")
    cell = wks.find(block_id)
    if cell:
        wks.update_cell(cell.row, 3, schedule_type)
        wks.update_cell(cell.row, 4, json.dumps(schedule_config, ensure_ascii=False))
        return True
    return False

def save_links_bulk(secrets_dict, block_id, df_links):
    """Lưu lại toàn bộ link của 1 khối sau khi sửa trên bảng"""
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_links")
    
    # 1. Xóa các link cũ của Block này
    all_vals = wks.get_all_values()
    # Giữ lại header (row 1) và các dòng KHÔNG thuộc block_id này
    # Cột Block ID là cột index 1 (B) -> row[1]
    new_data = [all_vals[0]] + [row for row in all_vals[1:] if row[1] != str(block_id)]
    
    # 2. Thêm các link mới từ DataFrame
    for _, row in df_links.iterrows():
        # Chuẩn hóa ngày tháng
        d_s = row.get("Date Start", "")
        d_e = row.get("Date End", "")
        if isinstance(d_s, (pd.Timestamp, datetime)): d_s = d_s.strftime("%Y-%m-%d")
        if isinstance(d_e, (pd.Timestamp, datetime)): d_e = d_e.strftime("%Y-%m-%d")
        
        new_row = [
            row.get("Link ID", str(uuid.uuid4())[:8]),
            str(block_id),
            row.get("Method", "GET"),
            row.get("API URL", ""),
            row.get("Access Token", ""),
            row.get("Link Sheet", ""),
            row.get("Sheet Name", ""),
            row.get("Filter Key", ""),
            str(d_s),
            str(d_e),
            row.get("Status", "Active")
        ]
        new_data.append(new_row)
    
    wks.clear()
    wks.update(new_data)
    return True

# --- CORE: LOGIC TẢI DỮ LIỆU (Tail Chaser + Day+1) ---
# (Giữ nguyên logic tối ưu từ phiên bản trước)
def build_manual_url(base_url, access_token, limit, page, filters_list=None):
    params = {"access_token": str(access_token).strip(), "limit": limit, "page": page, "sort_by": "id", "sort_type": "desc"}
    query_string = urlencode(params)
    filter_part = ""
    if filters_list:
        json_str = json.dumps(filters_list, separators=(',', ':'))
        filter_part = f"&filters={quote(json_str)}"
    return f"{base_url}?{query_string}{filter_part}"

def fetch_single_page_manual(full_url, method):
    try:
        if method.upper() == "POST": r = requests.post(full_url, json={}, timeout=30)
        else: r = requests.get(full_url, timeout=30)
        if r.status_code == 200:
            d = r.json()
            return d.get("data", d.get("items", []))
    except: pass
    return []

def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 50 # Limit cứng 50
    
    filters_list = None
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: 
            # Day+1 Strategy
            server_end_date = date_end + timedelta(days=1)
            f_obj[f"{filter_key}_to"] = server_end_date.strftime("%d/%m/%Y")
        filters_list = [f_obj]

    if status_callback: status_callback("📡 Gọi Page 1...")
    
    # Page 1
    page1_url = build_manual_url(url, token, limit, 1, filters_list)
    try:
        if method.upper() == "POST": res = requests.post(page1_url, json={}, timeout=30)
        else: res = requests.get(page1_url, timeout=30)
        
        if res.status_code != 200: return None, f"HTTP {res.status_code}"
        d = res.json()
        if d.get("code") == "token_not_valid": return None, "Hết hạn API"
        
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        if items: all_data.extend(items)
        if total_items == 0 and not items: return [], "Success (0 KQ)"

        # Tải song song
        estimated_pages = math.ceil(total_items / limit)
        if estimated_pages > 1:
            if status_callback: status_callback(f"🚀 Tải {estimated_pages} trang...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_single_page_manual, build_manual_url(url, token, limit, p, filters_list), method): p for p in range(2, estimated_pages + 1)}
                for future in as_completed(futures):
                    p_items = future.result()
                    if p_items: all_data.extend(p_items)

        # Vét cạn (Tail Chaser)
        current_page = estimated_pages + 1
        max_safety = 20
        while max_safety > 0:
            extra = fetch_single_page_manual(build_manual_url(url, token, limit, current_page, filters_list), method)
            if extra:
                all_data.extend(extra)
                current_page += 1
                max_safety -= 1
            else: break
            
        return all_data, "Success"
    except Exception as e: return None, str(e)

def write_to_sheet_range(secrets_dict, link_sheet, sheet_name, block_name, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(link_sheet)
        try: wks = dest_ss.worksheet(sheet_name)
        except: wks = dest_ss.add_worksheet(sheet_name, 1000, 20)
        
        wks.clear()
        
        if not data: return "0", "Empty Data"
        
        rows = [list(data[0].keys()) + ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]]
        month = datetime.now().strftime("%m/%Y")
        for item in data:
            r = list(item.values())
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([link_sheet, sheet_name, month, block_name])
            rows.append(r)
            
        wks.update(values=rows, range_name='A1')
        return f"Dòng 2 -> {len(rows)}", "Success"
    except Exception as e: return "0", str(e)

def log_system(secrets_dict, block_name, message, type_log="INFO"):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("log_system")
        now = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m")
        wks.append_row([now, block_name, message, type_log])
    except: pass
