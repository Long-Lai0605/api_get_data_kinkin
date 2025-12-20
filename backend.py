import requests
import pandas as pd
import utils
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time
from datetime import datetime

def call_1office_api(method, url, token, from_date=None, to_date=None):
    """
    Gọi API 1Office với cơ chế VÒNG LẶP (LOOP) qua các trang.
    Tự động tăng page lên cho đến khi không còn dữ liệu trả về.
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    all_data = [] # Nơi chứa tổng hợp dữ liệu tất cả các trang
    page = 1
    limit = 100   # 1Office thường giới hạn max 100 dòng/lần lấy
    
    # Thiết lập tham số cơ bản
    base_params = {'limit': limit}
    
    # Xử lý tham số ngày (nếu người dùng có nhập)
    if from_date and from_date != 'nan' and from_date != 'None': 
        base_params['from_date'] = from_date
    if to_date and to_date != 'nan' and to_date != 'None': 
        base_params['to_date'] = to_date

    print(f"🔄 Bắt đầu gọi API: {url}")
    
    try:
        while True:
            # Cập nhật số trang cho lần gọi này
            params = base_params.copy()
            params['page'] = page
            
            # Gửi Request
            try:
                if method.upper() == "POST":
                    resp = requests.post(url, headers=headers, json=params, timeout=45)
                else:
                    resp = requests.request(method.upper(), url, headers=headers, params=params, timeout=45)
            except requests.exceptions.RequestException as req_err:
                return None, f"Lỗi kết nối mạng: {str(req_err)}"

            # Kiểm tra HTTP Status
            if resp.status_code == 401: 
                return None, "Hết hạn API / Token sai (401)"
            if resp.status_code != 200: 
                return None, f"HTTP Error {resp.status_code}: {resp.text[:100]}"

            # Parse dữ liệu JSON
            try:
                data_json = resp.json()
            except:
                return None, "API không trả về JSON hợp lệ"

            # 1Office thường trả dữ liệu trong key 'data'. 
            # Cấu trúc: { "data": [...], "status": "success", ... }
            if isinstance(data_json, dict):
                items = data_json.get('data', [])
            elif isinstance(data_json, list):
                items = data_json
            else:
                items = []
            
            # --- ĐIỀU KIỆN DỪNG VÒNG LẶP ---
            
            # 1. Nếu không có item nào -> Hết dữ liệu -> Dừng
            if not items:
                print(f"   -> Trang {page}: Trống. Dừng.")
                break
                
            # Thêm dữ liệu trang này vào tổng
            all_data.extend(items)
            # print(f"   -> Trang {page}: Lấy được {len(items)} dòng.") # Bỏ comment để debug

            # 2. Nếu số lượng lấy về nhỏ hơn limit (VD: limit 100 mà chỉ lấy được 45) 
            # -> Đây là trang cuối cùng -> Dừng
            if len(items) < limit:
                break
            
            # Nếu chưa hết, tăng page lên để lấy tiếp vòng sau
            page += 1
            
            # Ngủ 0.2s để tránh spam server quá nhanh gây lỗi
            time.sleep(0.2)
            
        return pd.DataFrame(all_data), "Thành công"

    except Exception as e:
        return None, f"Lỗi Logic Loop: {str(e)}"

def process_sync(row_config, block_name):
    """
    Quy trình đồng bộ: Lấy Token thật -> Fetch (Loop) -> Xử lý -> Ghi Sheet
    """
    # 1. Lấy thông tin & Token Bảo mật
    url = str(row_config.get('API URL', '')).strip()
    if not url: return False, "Thiếu URL", 0
    
    # Lấy Token thật từ kho
    real_token = utils.get_real_token(block_name, url)
    if not real_token: 
        return False, "Chưa lưu Token vào kho bảo mật", 0
    
    method = str(row_config.get('Method', 'GET')).strip()
    target_link = str(row_config.get('Link Đích', '')).strip()
    sheet_name = str(row_config.get('Tên sheet dữ liệu dịch', 'Sheet1')).strip()
    
    # Lấy tham số ngày
    f_d = str(row_config.get('Ngày bắt đầu', ''))
    t_d = str(row_config.get('Ngày kết thúc', ''))

    # 2. GỌI API (Vòng lặp lấy hết dữ liệu)
    df, msg = call_1office_api(method, url, real_token, f_d, t_d)
    
    if df is None: return False, msg, 0
    if df.empty: return True, "API trả về 0 dòng dữ liệu", 0

    # 3. Chuẩn hóa dữ liệu trước khi ghi
    # Chuyển tất cả về string để tránh lỗi JSON khi ghi vào Sheet
    df = df.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')

    # Thêm 4 cột truy vết hệ thống (System Tracking Columns)
    df['Link file nguồn'] = url
    df['Sheet nguồn'] = "1Office_API"
    df['Tháng chốt'] = datetime.now().strftime("%m/%Y")
    df['Luồng'] = block_name

    # 4. Ghi vào Google Sheet (Cơ chế Tìm & Xóa cũ -> Ghi mới)
    try:
        creds = utils.get_creds()
        gc = utils.gspread.authorize(creds)
        
        # Mở Sheet Đích
        try:
            sh = gc.open_by_url(target_link)
        except Exception:
            return False, "Không mở được Link Đích (Sai link hoặc chưa cấp quyền Editor)", 0

        # Mở Tab Đích
        try: 
            wks = sh.worksheet(sheet_name)
        except: 
            # Nếu chưa có thì tạo mới
            wks = sh.add_worksheet(sheet_name, 1000, 20)
        
        # Đọc dữ liệu hiện tại trong Sheet Đích để lọc trùng
        existing = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        existing = existing.dropna(how='all') # Bỏ dòng trống
        
        # LOGIC QUAN TRỌNG: Tìm & Xóa dữ liệu cũ của URL này
        if 'Link file nguồn' in existing.columns:
            # Giữ lại những dòng KHÔNG PHẢI của URL này (Xóa cũ)
            existing = existing[existing['Link file nguồn'] != url]
        
        # Ghép dữ liệu cũ (đã lọc) + Dữ liệu mới vừa lấy (Append)
        final_df = pd.concat([existing, df], ignore_index=True)
        
        # Ghi đè lại toàn bộ Sheet
        wks.clear()
        set_with_dataframe(wks, final_df)
        
        return True, "Thành công", len(df) # Trả về số lượng dòng MỚI lấy được
    except Exception as e:
        return False, f"Lỗi Ghi Sheet: {str(e)}", 0
