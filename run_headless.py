import backend
import time
import uuid

def main():
    print(">>> KINKIN AUTOMATION: STARTING HEADLESS RUN...")
    
    # 1. Load Secrets (Local hoặc Environment)
    secrets = backend.load_secrets()
    if not secrets:
        print("❌ Lỗi: Không tìm thấy secrets.toml. Nếu chạy GitHub Actions hãy cấu hình ENV.")
        return

    # 2. Tạo Run ID định danh
    run_id = f"RUN-{uuid.uuid4().hex[:8].upper()}"
    backend.log_system_run(secrets, run_id, "START", "Bắt đầu tiến trình chạy ngầm")

    # 3. Lấy danh sách Block
    blocks = backend.get_active_blocks(secrets)
    print(f"📊 Tìm thấy {len(blocks)} cấu hình.")
    
    success_count = 0
    
    # 4. Chạy vòng lặp
    for block in blocks:
        name = block.get('Block Name')
        status = block.get('Trạng thái', '')
        
        # Chỉ chạy block chưa chốt
        if "Đã chốt" in status:
            print(f"⏩ Bỏ qua {name} (Đã chốt)")
            continue
            
        print(f"🔄 Đang xử lý: {name}...")
        
        # Gọi API
        data, msg = backend.fetch_1office_data(
            block['API URL'],
            block['Access Token (Encrypted)'],
            block['Method']
        )
        
        if msg == "Success" and data:
            # Ghi Sheet
            count, w_msg = backend.write_to_sheet(secrets, block, data)
            if count > 0:
                print(f"   ✅ {name}: +{count} dòng.")
                success_count += 1
            else:
                print(f"   ⚠️ {name}: Lỗi ghi sheet ({w_msg})")
        else:
            print(f"   ❌ {name}: Lỗi API ({msg})")
            
        time.sleep(1) # Nghỉ nhẹ

    # 5. Kết thúc
    print(">>> FINISHED.")
    backend.log_system_run(secrets, run_id, "END", f"Hoàn tất. Thành công: {success_count}/{len(blocks)}")

if __name__ == "__main__":
    main()
