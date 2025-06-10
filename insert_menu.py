import csv
import psycopg2


csv_file_path = '/home/phuonn/Shopee_food/UDPTDLTM_DATA/data/menu_data.csv'

def split_csv_file(input_file_path, num_splits=5):
    split_files = []
    try:
        with open(input_file_path, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Read the header row

            rows = list(reader)
            total_rows = len(rows)
            rows_per_file = total_rows // num_splits + (total_rows % num_splits > 0)
            
            for i in range(num_splits):
                split_file_path = f"{input_file_path.rsplit('.', 1)[0]}_part_{i+1}.csv"
                with open(split_file_path, 'w', newline='') as split_file:
                    writer = csv.writer(split_file)
                    writer.writerow(header)  # Write the header to each split file
                    writer.writerows(rows[i * rows_per_file: (i + 1) * rows_per_file])
                
                split_files.append(split_file_path)
        
        print("Tách file CSV thành công:", split_files)
    except Exception as e:
        print("Không thể tách file CSV:")
        print(e)
    return split_files

def connect_to_azure_postgres(host, database, user, password, port=5432):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        print("Kết nối thành công tới Azure PostgreSQL!")
        return conn
    except Exception as e:
        print("Không thể kết nối tới Azure PostgreSQL:")
        print(e)
        return None

def load_data_to_postgres(csv_files):
    try:
        # Kết nối đến PostgreSQL
        host = "shopee.postgres.database.azure.com"
        database = "delivery_info"
        user = "Numpy"
        password = "!Namphuong592003"
        conn = connect_to_azure_postgres(host, database, user, password)

        for csv_file_path in csv_files:
            with conn.cursor() as cursor:
                # Tạo bảng tạm để lưu trữ dữ liệu từ CSV
                cursor.execute("""
                    CREATE TEMP TABLE temp_menu (
                        restaurant_id INT,
                        menu TEXT
                    );
                """)

                # Sử dụng COPY để tải dữ liệu từ CSV vào bảng tạm
                with open(csv_file_path, 'r') as file:
                    next(file)  # Bỏ qua dòng tiêu đề
                    cursor.copy_expert("COPY temp_menu FROM STDIN WITH CSV", file)
                print(f"Dữ liệu từ {csv_file_path} đã được tải vào bảng tạm.")

                # Cập nhật và chèn dữ liệu từ bảng tạm vào bảng chính
                cursor.execute("""
                    UPDATE menu
                    SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
                    WHERE is_current = TRUE
                    AND EXISTS (
                        SELECT 1
                        FROM temp_menu
                        WHERE temp_menu.restaurant_id = menu.restaurant_id
                        AND temp_menu.menu IS NOT NULL
                        AND temp_menu.menu <> menu.menu
                    );
                """)
                print("Bản ghi cũ đã được cập nhật.")

                # Chèn các bản ghi mới hoặc đã thay đổi từ bảng tạm vào bảng chính
                cursor.execute("""
                    INSERT INTO menu (restaurant_id, menu, start_date, is_current)
                    SELECT restaurant_id, menu, CURRENT_TIMESTAMP, TRUE
                    FROM temp_menu
                    WHERE restaurant_id NOT IN (
                        SELECT restaurant_id FROM menu WHERE is_current = TRUE
                    );
                """)
                conn.commit()
                print(f"Dữ liệu mới từ {csv_file_path} đã được chèn vào bảng chính.")

                # Xóa bảng tạm sau khi xử lý xong
                cursor.execute("DROP TABLE IF EXISTS temp_menu;")

    except Exception as e:
        print("Không thể chèn dữ liệu vào Azure PostgreSQL:")
        print(e)
    finally:
        if conn:
            conn.close()

# Tách file CSV thành 5 phần và gọi hàm để tải dữ liệu từ từng phần
split_files = split_csv_file(csv_file_path, num_splits=10)
load_data_to_postgres(split_files)
