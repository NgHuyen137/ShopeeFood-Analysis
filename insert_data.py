import csv
import psycopg2
def connect_to_azure_postgres(host, database, user, password, port=5432):
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
        host = "shopee.postgres.database.azure.com"
        database = "delivery_info"
        user = "Numpy"
        password = "!Namphuong592003"
        conn = connect_to_azure_postgres(host, database, user, password)
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

        # Mở tệp CSV và chèn dữ liệu vào bảng
        # with open('./data/delivery_info_clean.csv', 'r', encoding='utf-8') as file:
        #     csv_reader = csv.reader(file)
        #     next(csv_reader)  

            # brands = {}
            # restaurant_data = []
            # review_data = []

            # for row in csv_reader:
            #     id, total_review, avg, restaurant_id, name, categories, cuisines, is_quality_merchant, latitude, longitude, brand_id, brand_name, url, district, avg_price = row
  
                # Kiểm tra xem brand_id đã tồn tại trong danh sách chưa
                # if brand_id not in brands:
                #     brands[brand_id] = brand_name

                # restaurant_data.append((
                #     restaurant_id, name, brand_id, cuisines, district, latitude, longitude, categories, is_quality_merchant.lower()
                # ))

                # Tích lũy dữ liệu vào danh sách review
            #     review_data.append((
            #         restaurant_id, total_review, avg
            #     ))
            #  # Chèn dữ liệu vào bảng Brand bằng cách sử dụng bulk insert
            # brand_data = [(brand_id, brand_name) for brand_id, brand_name in brands.items()]

            # if brand_data:
            #     cursor.executemany(
            #         """
            #         INSERT INTO brand (brand_id, brand_name)
            #         VALUES (%s, %s)
            #         ON CONFLICT (brand_id) DO NOTHING  -- Nếu brand_id đã tồn tại thì không làm gì cả
            #         """,
            #         brand_data
            #     )
            # Chèn theo lô vào bảng Restaurant
            # cursor.executemany(
            #     """
            #     INSERT INTO Restaurant (restaurant_id, name, brand_id, cuisines, district, latitude, longitude, category, is_quality_merchant)
            #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            #     """,
            #     restaurant_data
            # )

            # # Chèn theo lô vào bảng Review
            # cursor.executemany(
            #     """
            #     INSERT INTO Review (restaurant_id, total_review, avg_review)
            #     VALUES (%s, %s, %s)
            #     """,
            #     review_data
            # )

        conn.commit()
        cursor.execute("DROP TABLE IF EXISTS temp_restaurant_data;")
        cursor.close()
        print("Dữ liệu đã được chèn thành công.")

    except Exception as e:
        print("Không thể chèn dữ liệu vào Azure PostgreSQL:")
        print(e)

if __name__ == "__main__":
    insert_data_to_postgres()