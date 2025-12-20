import pandas as pd
import utils
import backend
from gspread_dataframe import set_with_dataframe, get_as_dataframe

print("🚀 START GITHUB AUTO RUN...")
try:
    utils.init_db()
    if utils.check_lock("GitHub"):
        print("🔒 Locked. Exit.")
        exit()
    utils.set_lock("GitHub", True)

    sh = utils.get_master_sh()
    wks_config = sh.worksheet(utils.SH_CONFIG)
    df_config = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
    
    count_job = 0
    # Chạy tất cả các dòng chưa chốt
    for idx, row in df_config.iterrows():
        if row.get("Trạng thái") == "Chưa chốt & đang cập nhật":
            print(f"Running: {row.get('Block_Name')} - {row.get('API URL')}")
            # Logic lấy Token thật đã được tích hợp trong backend.process_sync
            ok, msg, count = backend.process_sync(row, row.get('Block_Name'))
            
            df_config.at[idx, "Kết quả"] = msg
            df_config.at[idx, "Dòng dữ liệu"] = count
            if ok: count_job += 1

    wks_config.clear()
    set_with_dataframe(wks_config, df_config)
    
    utils.write_log(f"GitHub Auto: Processed {count_job} jobs", "GitHub")
    print("✅ DONE.")

except Exception as e:
    print(f"❌ ERROR: {e}")
    utils.write_log(f"Error: {e}", "GitHub")
finally:
    utils.set_lock("GitHub", False)
