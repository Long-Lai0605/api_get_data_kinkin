import gspread
import requests
import pandas as pd
import math
import time
import json
import uuid
import re
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote
from gspread_dataframe import set_with_dataframe, get_as_dataframe

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

# --- HELPER CLEAN ID (QUAN TRỌNG) ---
def clean_str(val):
    """Chuyển đổi mọi giá trị về string chuẩn để so sánh"""
    if pd.isna(val) or val is None: return ""
    return str(val).strip().replace(".0", "")

def safe_get_records(wks):
    try: return wks.get_all_records()
    except: return []

# --- INIT DATABASE ---
def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    # Định nghĩa Schema chuẩn
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
        creds_info = secrets_dict["gcp_service_account"]
        bot_email = creds_info.get("client_email", "unknown")
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
        gc = gspread.authorize(creds)
        gc.open_by_url(sheet_url)
        return True, "✅ Đã có quyền truy cập", bot_email
    except Exception as e: return False, f"⚠️ Lỗi: {str(e)}", ""

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
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    return safe_get_records(sh.worksheet("manager_blocks"))

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    try:
        wks = sh.worksheet("manager_links")
        data = wks.get_all_records()
        target_id = clean_str(block_id)
        return [l for l in data if clean_str(l.get("Block ID", "")) == target_id]
    except: return []

# --- UPDATE CONFIG ---
def update_block_config_and_schedule(secrets_dict, block_id, block_name, schedule_type, schedule_config):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    json_config = json.dumps(schedule_config, ensure_ascii=False)
    try:
        wks_b = sh.worksheet("manager_blocks")
        cell = wks_b.find(block_id)
        if cell:
            wks_b.update_cell(cell.row, 3, schedule_type)
            wks_b.update_cell(cell.row, 4, json_config)
    except: pass
    return True

# --- UPDATE REALTIME (LOGIC MATCHING CHUẨN XÁC) ---
def update_link_last_range(secrets_dict, link_id, block_id, range_val):
    """
    Tìm dòng trong manager_links có Link ID và Block ID khớp để update cột Last Range
    """
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("manager_links")
        
        # Lấy toàn bộ data để quét thủ công (tránh lỗi find của gspread)
        all_rows = wks.get_all_values()
        if not all_rows: return False

        # Xác định vị trí cột
        header = all_rows[0]
        try: 
            # Tìm cột Last Range (Cột L)
            col_idx_range = header.index("Last Range") + 1
            # Tìm cột Link ID và Block ID
            col_idx_link = header.index("Link ID")
            col_idx_block = header.index("Block ID")
        except: 
            # Fallback nếu header không chuẩn
            col_idx_range = 12
            col_idx_link = 0
            col_idx_block = 1

        target_link = clean_str(link_id)
        target_block = clean_str(block_id)
        
        # Quét từng dòng (bỏ qua header)
        for i, row in enumerate(all_rows[1:], start=2):
            # Lấy giá trị tại cột Link ID và Block ID
            current_link = clean_str(row[col_idx_link]) if len(row) > col_idx_link else ""
            current_block = clean_str(row[col_idx_block]) if len(row) > col_idx_block else ""
            
            # MATCHING: Cả 2 phải trùng khớp
            if current_link == target_link and current_block == target_block:
                wks.update_cell(i, col_idx_range, str(range_val))
                return True
        
        print(f"Warning: Không tìm thấy dòng LinkID={target_link} BlockID={target_block} để update range.")
        return False
    except Exception as e: 
        print(f"Error update range: {e}")
        return False

def log_execution_history(secrets_dict, block_name, trigger_type, status, details):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("log_lan_thuc_thi")
        now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m/%Y")
        wks.append_row([now_str, block_name, trigger_type, status, details])
    except: pass

# --- SAVE LINKS ---
def save_links_bulk(secrets_dict, block_id, df_links):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks = sh.worksheet("manager_links")
    
    old_df = get_as_dataframe(wks, evaluate_formulas=True).dropna(how='all')
    target_block = clean_str(block_id)
    
    if not old_df.empty and 'Block ID' in old_df.columns:
        old_df['Block ID'] = old_df['Block ID'].apply(clean_str)
        other_blocks_df = old_df[old_df['Block ID'] != target_block]
    else:
        other_blocks_df = pd.DataFrame()

    df_links['Block ID'] = target_block
    final_df = pd.concat([other_blocks_df, df_links], ignore_index=True)
    
    wks.clear()
    set_with_dataframe(wks, final_df)
    return True

# --- FETCH API ---
def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    filters_list = []
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: f_obj[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        if f_obj: filters_list.append(f_obj)

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

    if status_callback: status_callback("📡 Gọi Server...")
    items, total_items = fetch_page(1)
    if items:
        all_data.extend(items)
        if total_items > limit:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_page, p): p for p in range(2, math.ceil(total_items/limit) + 1)}
                for future in as_completed(futures):
                    p_items, _ = future.result()
                    if p_items: all_data.extend(p_items)
    return all_data, "Success"

# --- CORE LOGIC V8 (TÍNH RANGE DỰA TRÊN DỮ LIỆU ĐÃ GHI) ---
def process_data_final_v8(secrets_dict, link_sheet_url, sheet_name, block_id, link_id_config, new_data, status_mode):
    if not new_data and status_mode != "Chưa chốt & đang cập nhật": 
        return "0", "No Data from API"
    
    try:
        # 1. KẾT NỐI
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(link_sheet_url)
        try: wks = dest_ss.worksheet(sheet_name)
        except: wks = dest_ss.add_worksheet(sheet_name, 1000, 20)
        
        # 2. ĐỌC DỮ LIỆU CŨ
        old_df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        old_df = old_df.dropna(how='all').dropna(axis=1, how='all')
        
        # Chuẩn hóa tên cột Meta
        meta_cols = ["Link Nguồn", "Sheet Nguồn", "Block ID", "Link ID Config", "Thời gian điền"]
        for col in meta_cols:
            if col not in old_df.columns: old_df[col] = ""

        # Chuẩn hóa giá trị cột ID trong Old DF để so sánh
        if "Block ID" in old_df.columns: old_df["Block ID"] = old_df["Block ID"].apply(clean_str)
        if "Link ID Config" in old_df.columns: old_df["Link ID Config"] = old_df["Link ID Config"].apply(clean_str)

        # 3. PHÂN VÙNG
        target_block = clean_str(block_id)
        target_link = clean_str(link_id_config)
        
        is_target = (old_df["Block ID"] == target_block) & (old_df["Link ID Config"] == target_link)
        
        safe_zone_df = old_df[~is_target]
        target_zone_df = old_df[is_target]
        
        # 4. CHUẨN BỊ DỮ LIỆU MỚI
        if new_data:
            new_df = pd.DataFrame(new_data).astype(str)
            api_columns = [c for c in new_df.columns if c not in meta_cols]
            
            # Thêm Meta Info
            now_str = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
            new_df["Link Nguồn"] = link_sheet_url
            new_df["Sheet Nguồn"] = sheet_name
            new_df["Block ID"] = target_block
            new_df["Link ID Config"] = target_link
            new_df["Thời gian điền"] = now_str
            
            pk = api_columns[0] if api_columns else new_df.columns[0]
        else:
            new_df = pd.DataFrame()
            pk = None
            api_columns = []

        # 5. XỬ LÝ 4 TRẠNG THÁI
        result_df = pd.DataFrame()

        if status_mode == "Chưa chốt & đang cập nhật":
            result_df = new_df

        elif status_mode == "Cập nhật dữ liệu cũ":
            if target_zone_df.empty or new_df.empty:
                result_df = target_zone_df
            else:
                common_ids = set(target_zone_df[pk]).intersection(set(new_df[pk]))
                updated_rows = new_df[new_df[pk].isin(common_ids)]
                kept_history = target_zone_df[~target_zone_df[pk].isin(common_ids)]
                result_df = pd.concat([kept_history, updated_rows], ignore_index=True)

        elif status_mode == "Cập nhật dữ liệu mới":
            if target_zone_df.empty:
                result_df = new_df
            elif new_df.empty:
                result_df = target_zone_df
            else:
                existing_ids = set(target_zone_df[pk])
                pure_new_rows = new_df[~new_df[pk].isin(existing_ids)]
                result_df = pd.concat([target_zone_df, pure_new_rows], ignore_index=True)

        else: # "Đã chốt"
            result_df = target_zone_df

        # 6. GỘP LẠI
        final_df = pd.concat([safe_zone_df, result_df], ignore_index=True)
        
        # Sắp xếp cột Meta về cuối
        current_cols = list(final_df.columns)
        final_meta = [c for c in current_cols if c in meta_cols]
        final_data = [c for c in current_cols if c not in meta_cols]
        ordered_cols = final_data + meta_cols
        final_df = final_df[[c for c in ordered_cols if c in final_df.columns]]

        # 7. GHI VÀO SHEET
        wks.clear()
        set_with_dataframe(wks, final_df)
        
        # 8. TÍNH RANGE (DỰA TRÊN DỮ LIỆU CUỐI CÙNG ĐÃ GHI)
        # Reset index để đảm bảo index khớp với số dòng
        final_df = final_df.reset_index(drop=True)
        
        # Tìm lại các dòng thuộc về Link ID này
        # Sử dụng clean_str để so sánh chắc chắn
        mask = (final_df["Link ID Config"].apply(clean_str) == target_link) & \
               (final_df["Block ID"].apply(clean_str) == target_block)
        
        indices = final_df.index[mask].tolist()
        
        if indices:
            start_row = min(indices) + 2 # +2 vì Header (1) + 0-based index (1)
            end_row = max(indices) + 2
            range_str = f"{start_row} - {end_row}"
        else:
            range_str = "No Data"
        
        return range_str, "Success"

    except Exception as e:
        return "0", str(e)
