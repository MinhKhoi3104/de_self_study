import os, sys
from pyspark.sql import SparkSession
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _001_config._00101_database_config import JDBC_URL_POSTGRES, PG_JDBC_JAR_PATH
from _002_utils._00201_utils import *

def create_sample_table():
    # Tạo Spark Session
    spark = SparkSession.builder \
                .appName("create_sample_table") \
                .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH) \
                .getOrCreate()
    
    # Định nghĩa câu SQL DDL tạo Schema
    sql_create_schema = """CREATE SCHEMA IF NOT EXISTS e_commerce;"""

    # Định nghĩa câu SQL DDL tạo table 
    sql_create_table = \
    """
    CREATE TABLE IF NOT EXISTS e_commerce.customer (
    customer_id int4 PRIMARY KEY, -- ID tự tăng, khóa chính
    full_name VARCHAR(100) NOT NULL, -- Tên đầy đủ
    date_of_birth DATE,              -- Ngày tháng năm sinh
    hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
    phone_number VARCHAR(20) UNIQUE, -- Số điện thoại (UNIQUE để không bị trùng)
    email VARCHAR(100) UNIQUE,       -- Email (UNIQUE để không bị trùng)
    gender VARCHAR(10)               -- Giới tính (Nam, Nữ, Khác)
    );
    """

    # Định nghĩa câu SQL DML insert data
    sql_insert_data = """
    INSERT INTO e_commerce.customer (customer_id,full_name, date_of_birth, hometown, phone_number, email, gender) VALUES
    ('1','Nguyễn Văn A', '1990-05-15', 'Hà Nội', '0901234567', 'nguyenvana@gmail.com', 'Nam'),
    ('2','Trần Thị B', '1995-12-01', 'TP. Hồ Chí Minh', '0918765432', 'tranb@yahoo.com', 'Nữ'),
    ('3','Lê Hoàng Cảnh', '1988-08-20', 'Đà Nẵng', '0975123987', 'canhlh@hotmail.com', 'Nam'),
    ('4','Phạm Thu Duyên', '2000-03-10', 'Hải Phòng', '0887654321', 'duyenpt@mail.com', 'Nữ'),
    ('5','Vũ Minh Đức', '1975-11-25', 'Nghệ An', '0966333222', 'ducvm@company.com', 'Nam'),
    ('6','Hoàng Mai Lan', '1999-07-07', 'Thừa Thiên Huế', '0944111000', 'lanhm@work.vn', 'Nữ'),
    ('7','Đặng Quốc Huy', '1993-01-30', 'Cần Thơ', '0898555777', 'huydq@server.net', 'Nam'),
    ('8','Bùi Tố Như', '2001-04-18', 'Gia Lai', '0932468135', 'nhubt@example.org', 'Nữ'),
    ('9','Trịnh Đình Khang', '1985-06-03', 'Thanh Hóa', '0987654321', 'khangtd@blog.com', 'Nam'),
    ('10','Mai Ngọc Chi', '1997-09-22', 'Bình Dương', '0912987654', 'chinm@webmail.vn', 'Nữ');
    """


    # Thực thi các câu lệnh SQL DDL
    print("""Bắt đầu thực thi câu lệnh tạo Schema...""")
    result_1 = execute_sql_ddl (
            spark = spark,
            sql_query = sql_create_schema,
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
        )
    if result_1:
        print("""✅Thực thi câu lệnh tạo Schema thành công !""")
    else:
        print("""❌Thực thi câu lệnh tạo Schema thất bại !""")

    # Thực thi các câu lệnh SQL DDL
    print("""Bắt đầu thực thi câu lệnh tạo Table...""")
    result_2 = execute_sql_ddl (
            spark = spark,
            sql_query = sql_create_table,
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
        )
    if result_2:
        print("""✅Thực thi câu lệnh tạo Table thành công !""")
    else:
        print("""❌Thực thi câu lệnh tạo Table thất bại !""")

    # Thực thi các câu lệnh SQL DML
    print("""Bắt đầu thực thi câu lệnh insert data...""")
    result_3 = execute_sql_ddl (
            spark = spark,
            sql_query = sql_insert_data,
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
        )
    
    if result_3:
        print("""✅Thực thi câu lệnh insert data thành công !""")
    else:
        print("""❌Thực thi câu lệnh insert data thất bại !""")

    # Dọn dẹp SparkSession
    spark.stop()
    return True

if __name__ == "__main__":
    success = create_sample_table()
    # 4. Trả về mã thoát (Exit Code)
    exit(0 if success else 1)