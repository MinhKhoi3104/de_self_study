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

def scd_type_4_demo(etl_date=None):
    try:
        # Tạo etl_date
        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Khởi tạo SparkSession
        spark = SparkSession.builder \
                .appName("scd_type_4_demo") \
                .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH) \
                .getOrCreate()
        
        # Tạo bảng target ở database nếu chưa tồn tại
        sql_create_target_tbl = """
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_scd4 (
            customer_id int4, -- ID 
            full_name VARCHAR(100), -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            phone_number VARCHAR(20), -- Số điện thoại 
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
        
        # Tạo bảng target temp
        sql_create_target_temp_tbl = """
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_scd4_temp (
            customer_id int4, -- ID 
            full_name VARCHAR(100), -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            phone_number VARCHAR(20), -- Số điện thoại 
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
        
        # Tạo bảng his để lưu data cũ
        sql_create_his_tbl = """
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_his_scd4 (
            customer_id int4,
            full_name VARCHAR(100), -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            phone_number VARCHAR(20), -- Số điện thoại 
            email VARCHAR(100),       -- Email 
            gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            start_date DATE,
            end_date DATE
            );
        """

        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_his_tbl,
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
                    table= "(SELECT * FROM e_commerce.customer_dim_scd4) a",
                    properties=JDBC_URL_POSTGRES["properties"])
        
        target_df.show(5, truncate=False)

        # Thực hiện thiết lập SCD Type 4
        # Đối với bảng Target lấy giá trị mới nhất như SCD 1
        ### Lọc ra dữ liệu insert và update
        cols = [col for col in target_df.columns if col not in ("etl_date")]
        insert_update_df = \
            source_df.select(cols).subtract(target_df.select(cols))
        
        insert_update_df = insert_update_df.withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))
        
        ### Lọc dữ liệu không đổi và bị delete
        no_change_and_delete_df = \
            target_df.select(cols).alias("t")\
            .join(insert_update_df.alias("iu"), col("t.customer_id") == col("iu.customer_id"), "left_anti")\
            .select("t.*")
        
        no_change_and_delete_df = no_change_and_delete_df.withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))
        
        new_target_df = insert_update_df.unionByName(no_change_and_delete_df)

        ### Kiểm tra dữ liệu target mới
        print("New Target table...")
        new_target_df.show(5, truncate=False)

        ### Lấy các dữ liệu cũ
        old_records_df = target_df.select(cols).subtract(new_target_df.select(cols))

        old_records_df = \
            old_records_df.alias("o")\
            .join(target_df.alias("t"), col("o.customer_id") == col("t.customer_id"), "inner")\
            .select(
                "o.*",
                col("t.etl_date").alias("start_date"),
                to_date(lit(etl_date), "yyyyMMdd").alias("end_date")
            )
        
        print("Dữ liệu mới append vào bảng his...")
        old_records_df.orderBy(col("start_date").desc()).show(5,truncate=False)


        # Thực hiện đẩy dữ liệu mới vào bảng target theo SCD 4
        try:
            # Đẩy dữ liệu mới vài bảng target temp
            new_target_df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "e_commerce.customer_dim_scd4_temp") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd4_temp.")

            # 2. Truncate dữ liệu cũ tại bảng target
            truncate_success = execute_sql_ddl (
            spark = spark,
            sql_query = "truncate table e_commerce.customer_dim_scd4",
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
            )

            # KIỂM TRA KẾT QUẢ CỦA LỆNH SQL ĐẦU TIÊN
            if not truncate_success:
                # Nếu thất bại, ném lỗi để khối except ngoài cùng bắt lấy
                raise Exception("SQL Error: Lệnh TRUNCATE thất bại.") 
            print(f"✅ TRUNCATE bảng e_commerce.customer_dim_scd4 thành công.")

            # 3. Đưa dữ liệu mới vào bảng Target
            insert_success = execute_sql_ddl (
            spark = spark,
            sql_query = "insert into e_commerce.customer_dim_scd4 select * from e_commerce.customer_dim_scd4_temp",
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
            )

            # KIỂM TRA KẾT QUẢ CỦA LỆNH SQL THỨ HAI
            if not insert_success:
                raise Exception("SQL Error: Lệnh INSERT INTO SELECT thất bại.")
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd4.")

            # Ghi dữ liệu vào bảng His
            old_records_df.write \
                .format("jdbc") \
                .mode("append") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "e_commerce.customer_dim_his_scd4") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_his_scd4.")

        except Exception as e:
            print(f"❌ Ghi dữ liệu thất bại!")
            print(f"ERROR CHI TIẾT: {e}")
            return False


    except Exception as e:
        print(f"ERROR: {e}")
        return False
    
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='job scd_type_4_demo')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = scd_type_4_demo(etl_date=args.etl_date)
    exit(0 if success else 1)