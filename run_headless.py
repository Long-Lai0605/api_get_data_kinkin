import backend as be
import pandas as pd
import json
import time
from datetime import datetime, timedelta

# --- CẤU HÌNH ---
WEEKDAY_MAP = {
    "Thứ 2": 0, "Thứ 3": 1, "Thứ 4": 2, "Thứ 5": 3, 
    "Thứ 6": 4, "Thứ 7": 5, "CN": 6, "Chủ Nhật": 6
}

def load_secrets_local():
    """
    Hàm này tự đọc file secrets.json ngay tại đây
    thay vì gọi từ backend để tránh lỗi AttributeError.
    """
    try:
        with open("secrets.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ Lỗi: Không tìm thấy file secrets.json")
        return None
    except Exception as e:
        print(f"❌ Lỗi đọc JSON: {str(e)}")
        return None

def get_now_vn():
    return datetime.utcnow() + timedelta(hours=7)

def parse_time_str(time_str):
    try: return datetime.strptime(str(time_str).strip(), "%H:%M:%S").time()
    except:
        try: return datetime.strptime(str(time_str).strip(), "%H:%M").time()
        except: return None

def update_block_last_run(secrets, block_id, run_time_str):
    try:
        gc, _ = be.get_connection(secrets)
        wks = gc.worksheet("manager_blocks")
        cell = wks.find(block_id)
        if cell:
            wks.update_cell(cell.row, 6, run_time_str)
    except Exception as e:
        print(f"⚠️ Không thể update Last Run: {e}")

def should_run_block(block, now):
    # (Logic kiểm tra giờ chạy - Giữ nguyên như cũ)
    b_name = block.get("Block Name")
    sch_type = block.get("Schedule Type")
    sch_config_str = block.get("Schedule Config", "{}")
    last_run_str = str(block.get("Last Run", "")).strip()
    status = block.get("Status", "Active")

    if status != "Active": return False
    if sch_type == "Thủ công": return False

    last_run = datetime.min
    if last_run_str:
        try: last_run = datetime.strptime(last_run_str, "%H:%M:%S %d/%m/%Y")
        except: pass

    try:
        config = json.loads(sch_config_str) if isinstance(sch_config_str, str) else sch_config_str
    except:
        print(f"⚠️ {b_name}: Config lỗi JSON")
        return False

    # 1. HÀNG NGÀY
    if sch_type == "Hàng ngày":
        if "loop_minutes" in config and config["loop_minutes"] > 0:
            minutes_diff = (now - last_run).total_seconds() / 60
            if minutes_diff >= config["loop_minutes"]:
                print(f"✅ {b_name}: Đủ thời gian lặp ({int(minutes_diff)}/{config['loop_minutes']}p)")
                return True
        if "fixed_time" in config:
            target_time = parse_time_str(config["fixed_time"])
            if target_time:
                is_time_passed = now.time() >= target_time
                is_run_today = last_run.date() == now.date()
                if is_time_passed and not is_run_today:
                    print(f"✅ {b_name}: Đến giờ cố định {config['fixed_time']}")
                    return True

    # 2. HÀNG TUẦN
    elif sch_type == "Hàng tuần":
        current_weekday = now.weekday()
        if "run_1" in config:
            r = config["run_1"]
            target_wd = WEEKDAY_MAP.get(r.get("day", ""), -99)
            target_time = parse_time_str(r.get("time", ""))
            if current_weekday == target_wd and target_time:
                if now.time() >= target_time and last_run.date() != now.date():
                    return True
        if "run_2" in config:
            r = config["run_2"]
            target_wd = WEEKDAY_MAP.get(r.get("day", ""), -99)
            target_time = parse_time_str(r.get("time", ""))
            if current_weekday == target_wd and target_time:
                if now.time() >= target_time and last_run.date() != now.date():
                    return True

    # 3. HÀNG THÁNG
    elif sch_type == "Hàng tháng":
        current_day = now.day
        if "run_1" in config:
            r = config["run_1"]
            target_day = int(r.get("day", -1))
            target_time = parse_time_str(r.get("time", ""))
            if current_day == target_day and target_time:
                if now.time() >= target_time and last_run.date() != now.date():
                    return True
        if "run_2" in config:
            r = config["run_2"]
            target_day = int(r.get("day", -1))
            target_time = parse_time_str(r.get("time", ""))
            if current_day == target_day and target_time:
                if now.time() >= target_time and last_run.date() != now.date():
                    return True
    return False

# --- MAIN ---
def main():
    print(">>> KINKIN AUTOMATION: STARTING HEADLESS RUN...")
    
    # 1. Load Secrets tại chỗ (SỬA LỖI Ở ĐÂY)
    secrets = load_secrets_local()
    if not secrets:
        print("❌ CRITICAL: Không load được secrets. Dừng chương trình.")
        return

    # 2. Lấy danh sách Block từ Backend
    try:
        blocks = be.get_all_blocks(secrets)
    except Exception as e:
        print(f"❌ Lỗi kết nối Backend: {e}")
        return

    if not blocks:
        print("📭 Không có block nào trong hệ thống.")
        return

    now = get_now_vn()
    print(f"🕒 Time Check (VN): {now.strftime('%H:%M:%S %d/%m/%Y')}")

    for block in blocks:
        b_id = block.get("Block ID")
        b_name = block.get("Block Name")
        
        if should_run_block(block, now):
            print(f"▶️ KÍCH HOẠT CHẠY BLOCK: {b_name}...")
            
            links = be.get_links_by_block(secrets, b_id)
            for l in links:
                if l.get('Status') == "Đã chốt": continue
                
                sheet_name = l.get('Sheet Name')
                print(f"   ↳ Xử lý sheet: {sheet_name}")
                
                ds, de = None, None
                try:
                    if l.get('Date Start'): ds = pd.to_datetime(l.get('Date Start'), dayfirst=True).date()
                    if l.get('Date End'): de = pd.to_datetime(l.get('Date End'), dayfirst=True).date()
                except: pass

                data, msg = be.fetch_1office_data_smart(l['API URL'], l['Access Token'], 'GET', l['Filter Key'], ds, de, None)
                
                if msg == "Success":
                    r_str, w_msg = be.process_data_final_v11(secrets, l['Link Sheet'], sheet_name, b_id, l['Link ID'], data, l.get('Status'))
                    if "Error" not in w_msg:
                        be.update_link_last_range(secrets, l['Link ID'], b_id, r_str)
                        # GHI LOG
                        be.log_execution_history(secrets, b_name, sheet_name, "Auto (Headless)", "Success", r_str, "OK")
                        print(f"     ✅ Success: {r_str}")
                    else:
                        be.log_execution_history(secrets, b_name, sheet_name, "Auto (Headless)", "Error", "Fail", w_msg)
                        print(f"     ❌ Write Error: {w_msg}")
                else:
                    be.log_execution_history(secrets, b_name, sheet_name, "Auto (Headless)", "Error", "Fail", msg)
                    print(f"     ❌ API Error: {msg}")
                
                time.sleep(1)

            # Cập nhật Last Run cho Block
            now_str = now.strftime("%H:%M:%S %d/%m/%Y")
            update_block_last_run(secrets, b_id, now_str)
            print(f"🏁 Đã cập nhật Last Run cho {b_name}")

    print("✅ HEADLESS RUN COMPLETED.")

if __name__ == "__main__":
    main()
