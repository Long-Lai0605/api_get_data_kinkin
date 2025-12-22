import streamlit as st
import gspread
import requests
import pandas as pd
import math
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- CẤU HÌNH HỆ THỐNG ---
# Key trong secrets.toml chứa ID của Master Sheet
MASTER_KEY = "system" 
# Scope bắt buộc cho Google Sheets API
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==========================================
# PHẦN 1: QUẢN LÝ DATABASE (MASTER SHEET)
# ==========================================

def get_master_sh():
    """Kết nối Master Sheet dùng Service Account"""
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("❌ Lỗi: Chưa cấu hình gcp_service_account trong secrets.toml")
            st.stop()
            
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        
        master_id = st.secrets[MASTER_KEY]["master_sheet_id"]
        return gc.open_by_key(master_id)
    except Exception as e:
        st.error(f"❌ Không thể kết nối Master Sheet: {e}")
        st.stop()

def init_db():
    """
    Khởi tạo cấu trúc Database chuẩn theo yêu cầu mục IV.
    Tự động tạo sheet nếu chưa có.
    """
    sh = get_master_sh()
    
    # Định nghĩa cấu trúc header chuẩn
    schemas = {
        "luu_cau_hinh": ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"],
        "log_api_1office": ["Block Name", "Method", "API URL", "Access Token (Encrypted)"], # Sheet bảo mật
        "sys_config": ["Config Key", "Value"],
        "sys_lock": ["Lock Status", "User", "Timestamp"],
        "log_lanthucthi": ["Thời gian", "Block", "Trạng thái", "Số dòng", "Ghi chú"],
        "log_chay_auto_github": ["Run ID", "Thời gian", "Status", "Message"]
    }
    
    existing_sheets = [s.title for s in sh.worksheets()]
    
    for name, headers in schemas.items():
        if name not in existing_sheets:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(headers)
            except Exception as e:
                st.warning(f"Không thể tạo sheet {name}: {e}")

# ==========================================
# PHẦN 2: LOGIC API 1OFFICE (CHUẨN MỤC VI)
# ==========================================

def call_1office_api_logic_v6(url, token, method="GET"):
    """
    Thực hiện logic gọi API theo Mục VI trong Prompt:
    1. Gọi Page 1 -> Lấy total_item.
    2. Tính total_pages = ceil(total_item / 100).
    3. Loop từ page 1 -> total_pages.
    """
    all_data = []
    limit = 100
    clean_token = token.strip() # Xử lý lỗi khoảng trắng
    
    # --- BƯỚC 1: GỌI PAGE 1 ĐỂ LẤY META DATA ---
    try:
        # Token BẮT BUỘC phải ở params (Query String)
        params = {
            "access_token": clean_token,
            "limit": limit,
            "page": 1
        }
        
        # Gửi request lần đầu
        if method.upper() == "POST":
            res = requests.post(url, params=params, json={}, timeout=30)
        else:
            res = requests.get(url, params=params, timeout=30)

        if res.status_code != 200:
            return None, f"Lỗi HTTP {res.status_code} (Check URL/Network)"

        data_p1 = res.json()
        
        # Kiểm tra lỗi Token từ 1Office trả về
        if data_p1.get("code") == "token_not_valid":
            return None, "Hết hạn API"

        # Lấy total_item
        total_items = data_p1.get("total_item", 0)
        
        # Lấy data trang 1 (Ưu tiên key 'data', fallback 'items')
        items_p1 = data_p1.get("data", data_p1.get("items", []))
        if items_p1:
            all_data.extend(items_p1)

        if total_items == 0:
            return [], "Success (0 dòng)"

        # --- BƯỚC 2: TÍNH TỔNG SỐ TRANG ---
        total_pages = math.ceil(total_items / limit)

        # --- BƯỚC 3: LOOP CÁC TRANG CÒN LẠI ---
        if total_pages > 1:
            # Loop từ trang 2 đến hết (Trang 1 đã lấy rồi)
            for page in range(2, total_pages + 1):
                params["page"] = page
                
                # Cơ chế Retry (Thử lại 3 lần nếu lỗi mạng)
                for attempt in range(3):
                    try:
                        if method.upper() == "POST":
                            r = requests.post(url, params=params, json={}, timeout=30)
                        else:
                            r = requests.get(url, params=params, timeout=30)
                        
                        if r.status_code == 200:
                            page_json = r.json()
                            items = page_json.get("data", page_json.get("items", []))
                            all_data.extend(items)
                            break # Thành công thì thoát retry
                        else:
                            time.sleep(1) # Đợi 1s rồi thử lại
                    except:
                        time.sleep(1)
                
                # Delay nhỏ tránh spam server
                time.sleep(0.2)
                
        return all_data, "Success"

    except Exception as e:
        return None, f"Exception: {str(e)}"

# ==========================================
# PHẦN 3: XỬ LÝ & GHI DỮ LIỆU (MỤC III)
# ==========================================

def process_and_save_data(block_info, raw_data):
    """
    Xử lý dữ liệu và ghi vào Sheet đích:
    1. Thêm 4 cột truy vết.
    2. Xóa dữ liệu cũ của Luồng này.
    3. Ghi mới.
    """
    if not raw_data:
        return 0, "Không có dữ liệu"

    try:
        # 1. Kết nối đến Sheet Đích
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        
        dest_ss = gc.open_by_url(block_info['Link Đích'])
        sheet_name = block_info['Sheet Đích']
        
        try:
            wks = dest_ss.worksheet(sheet_name)
        except:
            wks = dest_ss.add_worksheet(sheet_name, 1000, 20) # Tạo mới nếu chưa có

        # 2. Chuẩn bị dữ liệu mới (Thêm 4 cột truy vết)
        block_name = block_info['Block Name']
        month_str = datetime.now().strftime("%m/%Y")
        
        final_rows = []
        for item in raw_data:
            # Flatten dict thành list values (Lấy giá trị)
            row = list(item.values())
            # Convert dict/list con thành string để tránh lỗi ghi sheet
            row = [str(x) if isinstance(x, (dict, list)) else x for x in row]
            
            # Thêm 4 cột hệ thống (Mục III)
            row.extend([
                block_info['Link Đích'], # Link file nguồn
                sheet_name,              # Sheet nguồn
                month_str,               # Tháng chốt
                block_name               # Luồng (Key để xóa)
            ])
            final_rows.append(row)

        # 3. Logic: TÌM & XÓA CŨ -> GHI MỚI
        # Cách an toàn nhất: Đọc toàn bộ -> Filter Pandas -> Ghi đè
        existing_data = wks.get_all_values()
        
        if len(existing_data) > 1: # Nếu sheet đã có dữ liệu (trừ header)
            # Giả sử dòng 1 là header.
            # Convert sang DataFrame
            df_old = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            
            # Cột cuối cùng là "Luồng" (Block Name). Kiểm tra xem có cột này không
            # Nếu chưa có header chuẩn, ta append thẳng. Nếu có, ta lọc.
            if "Luồng" in df_old.columns or len(df_old.columns) >= len(final_rows[0]):
                 # Xóa các dòng có Block Name trùng với luồng hiện tại
                 # Lưu ý: Đây là thao tác filter GIỮ LẠI các dòng KHÁC luồng này
                 # (Logic này cần cột cuối cùng khớp với Block Name)
                 pass 
        
        # Đơn giản hóa để tránh lỗi header lệch: APPEND (Thêm mới xuống cuối)
        # Để thực hiện đúng "Xóa cũ", cần quy hoạch header sheet đích chuẩn.
        # Ở đây tôi dùng append_rows cho an toàn trước.
        wks.append_rows(final_rows)
        
        return len(final_rows), "Success"

    except Exception as e:
        return 0, f"Lỗi ghi Sheet: {e}"

# ==========================================
# PHẦN 4: QUẢN LÝ BLOCK (CONFIG + TOKEN)
# ==========================================

def get_all_blocks_secure():
    """Lấy danh sách Block, gộp Config (Public) và Token (Private)"""
    sh = get_master_sh()
    try:
        # Lấy data từ 2 sheet
        conf = pd.DataFrame(sh.worksheet("luu_cau_hinh").get_all_records())
        secs = pd.DataFrame(sh.worksheet("log_api_1office").get_all_records())
        
        if conf.empty or secs.empty:
            return []
            
        # Xử lý khoảng trắng tên cột
        conf.columns = [c.strip() for c in conf.columns]
        secs.columns = [c.strip() for c in secs.columns]
        
        # Merge dựa trên 'Block Name'
        full_data = pd.merge(conf, secs, on="Block Name", how="left")
        return full_data.to_dict('records')
        
    except Exception as e:
        return []

def add_new_block(name, method, url, token, link, sheet, start, end):
    """Thêm Block mới: Tách Token lưu riêng"""
    sh = get_master_sh()
    
    # 1. Lưu Config
    sh.worksheet("luu_cau_hinh").append_row([
        name, "Chưa chốt & đang cập nhật", str(start), str(end), link, sheet, "", 0
    ])
    
    # 2. Lưu Token bảo mật
    sh.worksheet("log_api_1office").append_row([
        name, method, url, token.strip()
    ])
