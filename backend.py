import gspread
import requests
import pandas as pd
import math
import time
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

# --- HELPER ---
def safe_get_records(wks):
    try:
        data = wks.get_all_values()
        if not data: return []
        header = [str(h).strip() for h in data[0]]
        rows = data[1:]
        result = []
        for row in rows:
            item = {}
            for i, col_name in enumerate(header):
                val = row[i] if i < len(row) else ""
                item[col_name] = val
            result.append(item)
        return result
    except: return []

# --- INIT DATABASE ---
def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    schemas = {
        "manager_blocks": ["Block ID", "Block Name", "Schedule Type", "Schedule Config", "Status", "Last Run"],
        "manager_links": ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"],
        "log_system": ["Time", "Block", "Message", "Type"],
        "lich_chay_tu_dong": ["Block ID", "Block Name", "Frequency", "Config JSON", "Last Updated"],
        "log_lan_thuc_thi": ["Run Time", "Block Name", "Trigger Type", "Status", "Details"]
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass
        else:
            if name == "manager_links":
                try:
                    wks = sh.worksheet(name)
                    headers = wks.row_values(1)
                    if "Last Range" not in headers:
                        wks.update_cell(1, len(headers) + 1, "Last Range")
                except: pass

# --- QUYỀN TRUY CẬP ---
def check_sheet_access(secrets_dict, sheet_url):
    try:
        if not sheet_url or len(sheet_url) < 10:
            return False, "Link không hợp lệ", ""
        
        creds_info = secrets_dict["gcp_service_account"]
        bot_email = creds_info.get("client_email", "unknown")
        
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
        gc = gspread.authorize(creds)
        gc.open_by_url(sheet_url)
        return True, "✅ Đã có quyền truy cập", bot_email
        
    except gspread.exceptions.APIError:
        return False, "⛔ Chưa cấp quyền (403)", ""
    except Exception as e:
        return False, f"⚠️ Lỗi: {str(e)}", ""

# --- CRUD BLOCK ---
def create_block(secrets_dict, block_name):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks = sh.worksheet("manager_blocks")
    block_id = str(uuid.uuid4())[:8]
    wks.append_row([block_id, block_name, "Thủ công", "{}", "Active", ""])
    return True

def delete_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks_b = sh.worksheet("manager_blocks")
    cells = wks_b.findall(block_id)
    for r in sorted([c.row for c in cells], reverse=True): wks_b.delete_rows(r)
    
    wks_l = sh.worksheet("manager_links")
    all_vals = wks_l.get_all_values()
    if all_vals:
        rows_to_keep = [all_vals[0]]
        for row in all_vals[1:]:
            if len(row) > 1 and str(row[1]).strip() != str(block_id).strip():
                rows_to_keep.append(row)
        wks_l.clear()
        wks_l.update(rows_to_keep)
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    return safe_get_records(sh.worksheet("manager_blocks"))

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    all_links = safe_get_records(sh.worksheet("manager_links"))
    target_id = str(block_id).strip()
    return [l for l in all_links if str(l.get("Block ID", "")).strip() == target_id]

# --- UPDATE CONFIG & SCHEDULE ---
def update_block_config_and_schedule(secrets_dict, block_id, block_name, schedule_type, schedule_config):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    json_config = json.dumps(schedule_config, ensure_ascii=False)
    now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M %d/%m/%Y")

    wks_b = sh.worksheet("manager_blocks")
    cell = wks_b.find(block_id)
    if cell:
        wks_b.update_cell(cell.row, 3, schedule_type)
        wks_b.update_cell(cell.row, 4, json_config)

    try:
        wks_s = sh.worksheet("lich_chay_tu_dong")
        cell_s = wks_s.find(block_id)
        if cell_s:
            wks_s.update_cell(cell_s.row, 2, block_name)
            wks_s.update_cell(cell_s.row, 3, schedule_type)
            wks_s.update_cell(cell_s.row, 4, json_config)
            wks_s.update_cell(cell_s.row, 5, now_str)
        else:
            wks_s.append_row([block_id, block_name, schedule_type, json_config, now_str])
    except: pass     
    return True

def log_execution_history(secrets_dict, block_name, trigger_type, status, details):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("log_lan_thuc_thi")
        now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m/%Y")
        wks.append_row([now_str, block_name, trigger_type, status, details])
    except: pass

# --- UPDATE REALTIME (FIX LỖI: Match LinkID + BlockID) ---
def update_link_last_range(secrets_dict, link_id, block_id, range_val):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("manager_links")
        
        # 1. Tìm vị trí cột "Last Range" (Cột L hoặc khác nếu header thay đổi)
        header = wks.row_values(1)
        try:
            col_idx = header.index("Last Range") + 1
        except:
            col_idx = 12 # Mặc định nếu chưa có tiêu đề
            if len(header) < 12: 
                wks.update_cell(1, 12, "Last Range")

        # 2. Lấy toàn bộ dữ liệu để tìm dòng khớp cả 2 điều kiện
        all_vals = wks.get_all_values()
        
        target_link = str(link_id).strip()
        target_block = str(block_id).strip()
        
        row_idx_to_update = -1
        
        # Duyệt qua các dòng để tìm Match (Bỏ qua header)
        for i, row in enumerate(all_vals):
            # Cột 0 là Link ID, Cột 1 là Block ID
            if len(row) >= 2:
                current_link = str(row[0]).strip()
                current_block = str(row[1]).strip()
                
                if current_link == target_link and current_block == target_block:
                    row_idx_to_update = i + 1 # Gspread dùng index bắt đầu từ 1
                    break
        
        # 3. Ghi vào đúng dòng tìm thấy
        if row_idx_to_update != -1:
            wks.update_cell(row_idx_to_update, col_idx, str(range_val))
            return True
        else:
            print(f"Không tìm thấy dòng cho Link {link_id} - Block {block_id}")
            return False
            
    except Exception as e:
        print(f"Lỗi cập nhật range: {e}")
        return False

# --- SAVE LINKS ---
def save_links_bulk(secrets_dict, block_id, df_links):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    wks = sh.worksheet("manager_links")
    all_vals = wks.get_all_values()
    
    header = ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"]

    # Backup Last Range cũ (Map theo Link ID vì trong hàm này chỉ xử lý 1 Block ID cụ thể)
    old_data_map = {}
    if all_vals:
        try:
            lr_idx = all_vals[0].index("Last Range")
        except: lr_idx = 11

        target_block_id = str(block_id).strip()
        for r in all_vals[1:]:
            if len(r) > 1 and str(r[1]).strip() == target_block_id: # Chỉ backup của Block này
                if len(r) > lr_idx:
                    l_id = str(r[0]).strip()
                    old_data_map[l_id] = str(r[lr_idx])

    if not all_vals: 
        kept_rows = [header]
    else:
        target_block_id = str(block_id).strip()
        kept_rows = [all_vals[0]]
        if len(kept_rows[0]) < 12: kept_rows[0] = header

        for r in all_vals[1:]:
            if len(r) > 1 and str(r[1]).strip() != target_block_id:
                while len(r) < 12: r.append("")
                kept_rows.append(r)

    new_rows = []
    for i, (_, row) in enumerate(df_links.iterrows(), start=1):
        d_s = row.get("Date Start")
        d_e = row.get("Date End")
        
        l_id = str(row.get("Link ID", "")).strip()
        if not l_id: l_id = str(i)

        if pd.isna(d_s) or str(d_s).strip() == "": d_s = ""
        else:
            try: d_s = d_s.strftime("%Y-%m-%d")
            except: d_s = str(d_s)

        if pd.isna(d_e) or str(d_e).strip() == "": d_e = ""
        else:
            try: d_e = d_e.strftime("%Y-%m-%d")
            except: d_e = str(d_e)
        
        # Logic Restore
        curr_range = str(row.get("Last Range", "")).strip()
        if not curr_range:
            curr_range = old_data_map.get(l_id, "")

        r = [
            l_id,
            str(block_id).strip(),
            "GET", 
            row.get("API URL", ""),
            row.get("Access Token", ""),
            row.get("Link Sheet", ""),
            row.get("Sheet Name", ""),
            row.get("Filter Key", ""),
            str(d_s),
            str(d_e),
            row.get("Status", "Chưa chốt & đang cập nhật"),
            curr_range
        ]
        new_rows.append(r)
    
    wks.clear()
    wks.update(kept_rows + new_rows)
    return True

# --- FETCH & WRITE LOGIC ---
def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    filters_list = []
    
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: f_obj[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        if f_obj:
            filters_list.append(f_obj)
            if status_callback: status_callback(f"🎯 Server Filter: {json.dumps(f_obj)}")

    def fetch_page(p_idx):
        params = {"access_token": str(token).strip(), "limit": limit, "page": p_idx}
        if filters_list: params["filters"] = json.dumps(filters_list)
        try:
            full_query = urlencode(params)
            full_url = f"{url}?{full_query}"
            if method.upper() == "POST": r = requests.post(full_url, json={}, timeout=60)
            else: r = requests.get(full_url, timeout=60)
            if r.status_code == 200:
                d = r.json()
                return d.get("data", d.get("items", [])), d.get("total_item", 0)
            return [], 0
        except: return [], 0

    if status_callback: status_callback("📡 Gọi Server (Page 1)...")
    items, total_items = fetch_page(1)
    if status_callback: status_callback(f"📊 Server tìm thấy: {total_items} bản ghi.")
    
    if items:
        all_data.extend(items)
        if total_items > limit:
            estimated_pages = math.ceil(total_items / limit)
            if status_callback: status_callback(f"🚀 Đang tải thêm {estimated_pages - 1} trang...")
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_page, p): p for p in range(2, estimated_pages + 1)}
                for future in as_completed(futures):
                    p_items, _ = future.result()
                    if p_items: all_data.extend(p_items)
    return all_data, "Success"

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
        return f"{len(rows)}", "Success"
    except Exception as e: return "0", str(e)
