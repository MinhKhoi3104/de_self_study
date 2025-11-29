import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _001_config._00101_database_config import JDBC_URL_POSTGRES,PG_JDBC_JAR_PATH
from _002_utils._00201_utils import *

def scd_type_3_demo(etl_date=None):
    try:
        # Tạo etl_date
        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Khởi tạo SparkSession
        spark = SparkSession.builder \
                .appName("scd_type_3_demo") \
                .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH) \
                .getOrCreate()
        
        # Tạo bảng target ở database nếu chưa tồn tại
        sql_create_target_tbl = """
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_scd3 (
            customer_id int4, -- khóa chính
            full_name VARCHAR(100) NOT NULL, -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            pre_hometown VARCHAR(150),       -- Quê quán (hoặc địa chỉ) trước đây
            phone_number VARCHAR(20), -- Số điện thoại 
            pre_phone_number VARCHAR(20), -- Số điện thoại trước đây
            email VARCHAR(100),       -- Email 
            gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            etl_date DATE
            );
        """

        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_target_tbl,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Taoj bảng target temp
        sql_create_target_temp_tbl = """
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_scd3_temp (
            customer_id int4, -- khóa chính
            full_name VARCHAR(100) NOT NULL, -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            pre_hometown VARCHAR(150),       -- Quê quán (hoặc địa chỉ) trước đây
            phone_number VARCHAR(20), -- Số điện thoại 
            pre_phone_number VARCHAR(20), -- Số điện thoại trước đây
            email VARCHAR(100),       -- Email 
            gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            etl_date DATE
            );
        """

        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_target_temp_tbl,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )

        # Thực hiện gọi lấy dữ liệu bảng nguồn
        source_df = spark.read\
                    .jdbc(url=JDBC_URL_POSTGRES["url"],\
                    table= "(SELECT * FROM e_commerce.customer) a",
                    properties=JDBC_URL_POSTGRES["properties"])

        # Thêm cột etl_date vào tbl
        source_df = source_df\
            .withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))
        
        # Thực hiện kiểm tra dữ liệu 
        print("Kiểm tra dữ liệu nguồn...")
        source_df.show(5, truncate=False)
        
        # Kiểm tra target
        print("Kiểm tra dữ liệu target:")
        target_df = spark.read\
                    .jdbc(url=JDBC_URL_POSTGRES["url"],\
                    table= "(SELECT * FROM e_commerce.customer_dim_scd3) a",
                    properties=JDBC_URL_POSTGRES["properties"])
        
        target_df.show(5, truncate=False)

        # Thực hiện thiết lập SCD Type 3
        
        cols = [col for col in target_df.columns if col not in "etl_date"]
        col_to_minus = [col for col in source_df.columns if col not in "etl_date"]

        ### Chọn ra những dữ liệu thay đổi
        ### Các dòng khác thay đổi ngoài dòng được set SCD Type 3
        insert_df = \
            source_df.alias("s")\
            .join(target_df.alias("t"), col("s.customer_id") == col("t.customer_id"), "left_anti")\
            .select("s.*")

        insert_df = \
            insert_df\
            .withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))\
            .withColumn("pre_hometown", lit(None))\
            .withColumn("pre_phone_number", lit(None))\
            

        hometown_change_df = \
            source_df.alias("s")\
            .join(target_df.alias("t"), col("s.customer_id") == col("t.customer_id"), "inner")\
            .where(
                (col("s.hometown") != col("t.hometown"))
                & (col("s.phone_number") == col("t.phone_number"))
            )\
            .select(
                    col("s.customer_id"),
                    col("s.full_name"),
                    col("s.date_of_birth"),
                    col("s.hometown"),
                    col("t.hometown").alias("pre_hometown"),
                    col("s.phone_number"),
                    col("t.pre_phone_number"),
                    col("s.email"),
                    col("s.gender"),
                    col("s.etl_date")
            )
        
        phone_change_df = \
            source_df.alias("s")\
            .join(target_df.alias("t"), col("s.customer_id") == col("t.customer_id"), "inner")\
            .where(
                (col("s.hometown") == col("t.hometown"))
                & (col("s.phone_number") != col("t.phone_number"))
            )\
            .select(
                    col("s.customer_id"),
                    col("s.full_name"),
                    col("s.date_of_birth"),
                    col("s.hometown"),
                    col("t.pre_hometown"),
                    col("s.phone_number"),
                    col("t.phone_number").alias("pre_phone_number"),
                    col("s.email"),
                    col("s.gender"),
                    col("s.etl_date")
            )
        
        other_change_df =\
            source_df.alias("s")\
            .join(target_df.alias("t"), col("s.customer_id") == col("t.customer_id"), "inner")\
            .where(
                (col("s.hometown") == col("t.hometown"))
                & (col("s.phone_number") == col("t.phone_number"))
                & (
                    (col("s.full_name") != col("t.full_name"))
                    | (col("s.date_of_birth") != col("t.date_of_birth"))
                    | (col("s.email") != col("t.email"))
                    | (col("s.gender") != col("t.gender"))
                )
            )\
            .select(
                    col("s.customer_id"),
                    col("s.full_name"),
                    col("s.date_of_birth"),
                    col("s.hometown"),
                    col("t.pre_hometown"),
                    col("s.phone_number"),
                    col("t.pre_phone_number"),
                    col("s.email"),
                    col("s.gender"),
                    col("s.etl_date")
            )
        
        
        total_change_df = \
            hometown_change_df.select(cols)\
                .union(phone_change_df.select(cols))\
                    .union(other_change_df.select(cols))
        
        total_change_df = total_change_df.withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))
        
        no_change_df = \
            target_df.select(cols).alias("t")\
            .join(total_change_df.alias("u"), col("t.customer_id") == col("u.customer_id"), "left_anti")\
            .select("t.*")
        
        no_change_df = no_change_df.withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))

        new_target_df = no_change_df.unionByName(total_change_df).unionByName(insert_df)
        
        ### Kiểm tra dữ liệu target mới
        print("New Target table...")
        new_target_df.show(10, truncate=False)
        
        # Thực hiện đẩy dữ liệu mới vào bảng target theo SCD 3
        try:
            new_target_df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "e_commerce.customer_dim_scd3_temp") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd3_temp.")

            # 2. Truncate dữ liệu cũ tại bảng target
            truncate_success = execute_sql_ddl (
                spark = spark,
                sql_query = "truncate table e_commerce.customer_dim_scd3",
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
            
            # KIỂM TRA KẾT QUẢ CỦA LỆNH SQL ĐẦU TIÊN
            if not truncate_success:
                # Nếu thất bại, ném lỗi để khối except ngoài cùng bắt lấy
                raise Exception("SQL Error: Lệnh TRUNCATE thất bại.") 
            print(f"✅ TRUNCATE bảng e_commerce.customer_dim_scd3 thành công.")


            # 3. Đưa dữ liệu mới vào bảng Target
            insert_success = execute_sql_ddl (
                spark = spark,
                sql_query = "insert into e_commerce.customer_dim_scd3 select * from e_commerce.customer_dim_scd3_temp",
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
            
            # KIỂM TRA KẾT QUẢ CỦA LỆNH SQL THỨ HAI
            if not insert_success:
                raise Exception("SQL Error: Lệnh INSERT INTO SELECT thất bại.")
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd3.")


        except Exception as e:
            print(f"❌ Ghi dữ liệu thất bại!")
            print(f"ERROR CHI TIẾT: {e}")
            return False

        return True
    
    except Exception as e:
        print(f"ERROR: {e}")
        return False
    
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='job scd_type_3_demo')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = scd_type_3_demo(etl_date=args.etl_date)
    exit(0 if success else 1)