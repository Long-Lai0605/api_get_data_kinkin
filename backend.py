import requests
import pandas as pd
import utils
import time
import json
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# ------------------------------------------------------------------
# HÀM GỌI API CHẾ ĐỘ DEBUG (IN RA MỌI THỨ)
# ------------------------------------------------------------------
def call_1office_api(method, url, token, from_date=None, to_date=None, callback=None):
    headers = {
        'Authorization': f'Bearer {token}', 
        'Content-Type': 'application/json'
    }
    
    # Chỉ lấy trang 1 để kiểm tra
    params = {'limit': 100, 'page': 1}
    
    # Thêm tham số ngày nếu có
    if from_date and from_date not in ['nan', 'None', '']: params['from_date'] = from_date
    if to_date and to_date not in ['nan', 'None', '']: params['to_date'] = to_date

    # 1. IN RA THÔNG TIN GỬI ĐI
    if callback:
        callback(f"📡 Đang gửi request...")
        callback(f"👉 URL: `{url}`")
        callback(f"👉 Method: `{method}` (Cần POST cho /gets)")
        callback(f"👉 Params: `{json.dumps(params)}`")

    try:
        # Gửi Request (Hỗ trợ cả GET và POST để test)
        if method.upper() == "POST":
            resp = requests.post(url, headers=headers, json=params, timeout=30)
        else:
            resp = requests.request(method.upper(), url, headers=headers, params=params, timeout=30)

        # 2. IN RA KẾT QUẢ THÔ (RAW RESPONSE) - QUAN TRỌNG NHẤT
        if callback: callback(f"📩 HTTP Status Code: `{resp.status_code}`")
        
        # In 500 ký tự đầu tiên của phản hồi để xem lỗi
        raw_text = resp.text
        preview_text = raw_text[:500] + "..." if len(raw_text) > 500 else raw_text
        print(f"DEBUG RAW: {raw_text}") # In ra terminal console
        
        if callback: 
            callback(f"📝 **Nội dung API trả về:**")
            callback(f"```json\n{preview_text}\n```")

        # 3. PHÂN TÍCH LỖI
        try:
            data_json = resp.json()
        except:
            return None, "⛔ API không trả về JSON (Xem chi tiết ở trên)"

        # Kiểm tra Total
        total = data_json.get('total') or data_json.get('total_item') or 0
        items = data_json.get('data', [])
        
        if isinstance(data_json, dict) and data_json.get('status') == 'error':
             err_msg = data_json.get('message', 'Lỗi không xác định')
             return None, f"⛔ 1Office báo lỗi: {err_msg}"

        if total == 0 and not items:
            msg = "⚠️ Total = 0. "
            if method.upper() == "GET":
                msg += "Nguyên nhân cao nhất: Bạn đang dùng GET cho hàm /gets. Hãy đổi sang POST."
            else:
                msg += "Tài khoản có thể không có quyền xem dữ liệu này."
            return pd.DataFrame(), msg

        # Nếu có dữ liệu
        if callback: callback(f"✅ Tìm thấy dữ liệu! Total: {total}, Lấy được: {len(items)} dòng.")
        return pd.DataFrame(items), "Debug Thành công"

    except Exception as e:
        return None, f"⛔ Lỗi Code Debug: {str(e)}"


# ------------------------------------------------------------------
# HÀM XỬ LÝ CHÍNH (GIỮ NGUYÊN LOGIC KẾT NỐI SHEET)
# ------------------------------------------------------------------
def process_sync(row_config, block_name, callback=None):
    if callback: callback("🔑 Đang lấy Token bảo mật...")
    
    url = str(row_config.get('API URL', '')).strip()
    real_token = utils.get_real_token(block_name, url)
    
    if not real_token: 
        return False, "Thiếu Token trong kho bảo mật", 0
    
    method = str(row_config.get('Method', 'GET')).strip()
    target_link = str(row_config.get('Link Đích', '')).strip()
    sheet_name = str(row_config.get('Tên sheet dữ liệu dịch', 'Sheet1')).strip()
    f_d = str(row_config.get('Ngày bắt đầu', ''))
    t_d = str(row_config.get('Ngày kết thúc', ''))

    # GỌI HÀM DEBUG Ở TRÊN
    df, msg = call_1office_api(method, url, real_token, f_d, t_d, callback=callback)
    
    if df is None: return False, msg, 0
    if df.empty: return True, f"0 dòng ({msg})", 0

    # GHI RA SHEET
    if callback: callback(f"⚙️ Đang ghi {len(df)} dòng vào Sheet...")
    df = df.astype(str).replace(['nan', 'None'], '')
    df['Link file nguồn'] = url
    df['Sheet nguồn'] = "1Office"
    df['Tháng chốt'] = datetime.now().strftime("%m/%Y")
    df['Luồng'] = block_name

    try:
        creds = utils.get_creds()
        gc = utils.gspread.authorize(creds)
        sh = gc.open_by_url(target_link)
        try: wks = sh.worksheet(sheet_name)
        except: wks = sh.add_worksheet(sheet_name, 1000, 20)
        
        existing = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if 'Link file nguồn' in existing.columns:
            existing = existing[existing['Link file nguồn'] != url]
        
        final_df = pd.concat([existing, df], ignore_index=True)
        wks.clear()
        set_with_dataframe(wks, final_df)
        
        return True, "Thành công", len(df)
    except Exception as e:
        return False, f"Lỗi Ghi Sheet: {str(e)}", 0
