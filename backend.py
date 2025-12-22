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
        "lich_chay_tu_dong": ["Loại lịch", "Chi tiết", "Cập nhật lúc"] # Bảng mới lưu cấu hình hẹn giờ
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass

# --- [MỚI] HÀM LƯU CẤU HÌNH TỪ DASHBOARD ---
def save_configurations(secrets_dict, df_editor):
    """Lưu dữ liệu từ st.data_editor xuống sheet luu_cau_hinh"""
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("luu_cau_hinh")
        
        # Lấy dữ liệu cũ để giữ lại các cột không hiển thị trên dashboard (nếu có)
        # Ở đây ta giả định df_editor đã chứa đủ các cột cần thiết để overwrite
        
        # Chuẩn bị dữ liệu để ghi
        # df_editor là DataFrame đã edit
        # Cần đảm bảo thứ tự cột khớp với Schema:
        # ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Filter Key", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"]
        
        required_cols = ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Filter Key", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"]
        
        # Đảm bảo đủ cột (nếu thiếu thì fill rỗng)
        for col in required_cols:
            if col not in df_editor.columns:
                df_editor[col] = ""
                
        # Sắp xếp đúng thứ tự
        df_to_save = df_editor[required_cols]
        
        # Chuyển về list of lists
        # Lưu ý: Convert các kiểu ngày tháng/số về string để tránh lỗi JSON
        data_values = df_to_save.astype(str).values.tolist()
        
        # Xóa dữ liệu cũ (trừ header dòng 1)
        wks.clear()
        wks.append_row(required_cols)
        wks.append_rows(data_values)
        
        return True, "Đã lưu cấu hình thành công!"
    except Exception as e:
        return False, f"Lỗi lưu: {str(e)}"

# --- [MỚI] HÀM LƯU LỊCH CHẠY ---
def save_schedule_settings(secrets_dict, schedule_type, details_json):
    try:
        sh, _ = get_connection(secrets_dict)
        try: wks = sh.worksheet("lich_chay_tu_dong")
        except: wks = sh.add_worksheet("lich_chay_tu_dong", 100, 5)
        
        wks.clear()
        wks.append_row(["Loại lịch", "Chi tiết", "Cập nhật lúc"])
        
        now = datetime.now().strftime("%H:%M %d/%m/%Y")
        wks.append_row([schedule_type, json.dumps(details_json, ensure_ascii=False), now])
        return True
    except Exception as e: return False

# --- CÁC HÀM FETCH & XỬ LÝ (GIỮ NGUYÊN TỪ PHIÊN BẢN TRƯỚC) ---
def parse_date_val(date_str):
    if not date_str: return None
    s = str(date_str).strip()
    formats = ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"]
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
        if not val_date: 
            filtered.append(item)
            continue
        if d_start and val_date < d_start: continue
        if d_end and val_date > d_end: continue
        filtered.append(item)
    return filtered

def build_manual_url(base_url, access_token, limit, page, filters_list=None):
    params = {"access_token": access_token.strip(), "limit": limit, "page": page, "sort_by": "id", "sort_type": "desc"}
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
    limit = 50
    filters_list = None
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: f_obj[f"{filter_key}_to"] = (date_end + timedelta(days=1)).strftime("%d/%m/%Y")
        filters_list = [f_obj]

    if status_callback: status_callback("📡 Gọi Page 1...")
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

        estimated_pages = math.ceil(total_items / limit)
        if estimated_pages > 1:
            if status_callback: status_callback(f"🚀 Tải {estimated_pages} trang...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_single_page_manual, build_manual_url(url, token, limit, p, filters_list), method): p for p in range(2, estimated_pages + 1)}
                for future in as_completed(futures):
                    p_items = future.result()
                    if p_items: all_data.extend(p_items)

        # Vét cạn
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
        
        rows = [list(data[0].keys()) + ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]]
        month = datetime.now().strftime("%m/%Y")
        for item in data:
            r = list(item.values())
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, block_conf['Block Name']])
            rows.append(r)
        wks.update(values=rows, range_name='A1')
        
        range_str = f"Dòng 2 -> {len(rows)}"
        update_master_status(secrets_dict, block_conf['Block Name'], range_str)
        return range_str, "Success"
    except Exception as e: return "0", str(e)

def update_master_status(secrets_dict, block_name, range_str):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("luu_cau_hinh")
        cell = wks.find(block_name)
        if cell:
            vn_time = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M %d/%m")
            wks.update_cell(cell.row, 8, vn_time)
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
        full = pd.merge(c, s, on="Block Name", how="left")
        return full.fillna("").to_dict('records')
    except: return []

def add_new_block(secrets_dict, name, method, url, token, link, sheet, start, end, filter_key):
    sh, _ = get_connection(secrets_dict)
    sh.worksheet("luu_cau_hinh").append_row([name, "Chưa chốt & đang cập nhật", str(start), str(end), filter_key, link, sheet, "", ""])
    sh.worksheet("log_api_1office").append_row([name, method, url, token.strip()])
    return True
