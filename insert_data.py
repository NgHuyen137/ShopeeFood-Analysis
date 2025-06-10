import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")

def connect_to_azure_postgres(host, database, user, password, port):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        print(f"Kết nối thành công tới Azure PostgreSQL {database}!")
        return conn
    except Exception as e:
        print("Không thể kết nối tới Azure PostgreSQL:")
        print(e)
        return None
    
def insert_data_to_postgres():
    try:
        # Tạo cursor để thực thi lệnh SQL
        host = HOST
        database = DATABASE
        user = USER
        password = PASSWORD
        port = PORT
        conn = connect_to_azure_postgres(host, database, user, password, port)
        cursor = conn.cursor()
        cursor.execute("""
                    CREATE TEMP TABLE temp_restaurant_data (
                        restaurant_id INT,
                        avg_price NUMERIC
                    );
                """)
        with open('./data/temp_clean.csv', 'r', encoding='utf-8') as file:
            cursor.copy_expert("""
                    COPY temp_restaurant_data (restaurant_id, avg_price)
                    FROM STDIN
                    WITH CSV HEADER
            """, file)
        print(f"Dữ liệu đã được tải vào bảng tạm.")
        cursor.execute("""
                    UPDATE restaurant
                    SET avg_price = temp.avg_price
                    FROM temp_restaurant_data temp
                    WHERE restaurant.restaurant_id = temp.restaurant_id;
                """)
        print("Bản ghi cũ đã được cập nhật.")

        conn.commit()
        cursor.execute("DROP TABLE IF EXISTS temp_restaurant_data;")
        cursor.close()
        print("Dữ liệu đã được chèn thành công.")

    except Exception as e:
        print("Không thể chèn dữ liệu vào Azure PostgreSQL:")
        print(e)

if __name__ == "__main__":
    insert_data_to_postgres()