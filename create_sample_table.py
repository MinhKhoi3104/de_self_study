import os, sys
from pyspark.sql import SparkSession
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from config.database_config import JDBC_URL_POSTGRES, PG_JDBC_JAR_PATH

def execute_sql_ddl(spark, sql_query, jdbc_url, properties):
    """Thực thi câu lệnh SQL DDL/DML trực tiếp lên database qua kết nối JDBC."""
    
    jvm = spark._jvm
    
    try:
        DriverManager = jvm.java.sql.DriverManager
        connection = DriverManager.getConnection(
            jdbc_url, 
            properties["user"], 
            properties["password"]
        )
        statement = connection.createStatement()
        statement.execute(sql_query)
        print(f"✅ SQL DDL executed successfully: {sql_query}")
        return True
        
    except Exception as e:
        print(f"❌ Error executing SQL '{sql_query}': {e}")
        return False
        
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()

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
    customer_id SERIAL PRIMARY KEY, -- ID tự tăng, khóa chính
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
    INSERT INTO e_commerce.customer (full_name, date_of_birth, hometown, phone_number, email, gender) VALUES
    ('Nguyễn Văn A', '1990-05-15', 'Hà Nội', '0901234567', 'nguyenvana@gmail.com', 'Nam'),
    ('Trần Thị B', '1995-12-01', 'TP. Hồ Chí Minh', '0918765432', 'tranb@yahoo.com', 'Nữ'),
    ('Lê Hoàng Cảnh', '1988-08-20', 'Đà Nẵng', '0975123987', 'canhlh@hotmail.com', 'Nam'),
    ('Phạm Thu Duyên', '2000-03-10', 'Hải Phòng', '0887654321', 'duyenpt@mail.com', 'Nữ'),
    ('Vũ Minh Đức', '1975-11-25', 'Nghệ An', '0966333222', 'ducvm@company.com', 'Nam'),
    ('Hoàng Mai Lan', '1999-07-07', 'Thừa Thiên Huế', '0944111000', 'lanhm@work.vn', 'Nữ'),
    ('Đặng Quốc Huy', '1993-01-30', 'Cần Thơ', '0898555777', 'huydq@server.net', 'Nam'),
    ('Bùi Tố Như', '2001-04-18', 'Gia Lai', '0932468135', 'nhubt@example.org', 'Nữ'),
    ('Trịnh Đình Khang', '1985-06-03', 'Thanh Hóa', '0987654321', 'khangtd@blog.com', 'Nam'),
    ('Mai Ngọc Chi', '1997-09-22', 'Bình Dương', '0912987654', 'chinm@webmail.vn', 'Nữ');
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