from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
from datetime import datetime
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
    """
    Kết nối đến Azure PostgreSQL.

    Args:
        host (str): Địa chỉ server của Azure PostgreSQL.
        database (str): Tên cơ sở dữ liệu.
        user (str): Tên người dùng.
        password (str): Mật khẩu của người dùng.
        port (int, optional): Cổng của PostgreSQL (mặc định là 5432).

    Returns:
        conn: Đối tượng kết nối PostgreSQL nếu thành công, None nếu thất bại.
    """
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

# Hàm để chèn dữ liệu vào bảng PostgreSQL
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

def process_and_load_data_to_postgres():
    # Tách file CSV thành nhiều phần nhỏ
    csv_files = split_csv_file(csv_file_path, num_splits=10)  # Tùy chọn số phần muốn chia
    
    # Tải dữ liệu từ từng phần nhỏ vào PostgreSQL
    load_data_to_postgres(csv_files)

# Khởi tạo DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),
    'retries': 1,
}

with DAG(
    'crawl_menu_and_load_menu',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Task 1: Chạy script crawl_dish.py
    crawl_data = BashOperator(
        task_id='crawl_menu',
        bash_command='source /home/phuonn/anaconda3/etc/profile.d/conda.sh && conda activate min_ds-env && python /home/phuonn/Shopee_food/UDPTDLTM_DATA/crawl_menu.py'
    )

    # Task 2: Load dữ liệu vào Azure PostgreSQL
    load_to_postgres = PythonOperator(
        task_id='split_and_load_menu_to_postgres',
        python_callable=process_and_load_data_to_postgres
    )

    # Thực thi task
    crawl_data >> load_to_postgres
