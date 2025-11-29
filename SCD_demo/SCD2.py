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

def scd_type_2_demo(etl_date=None):
    try:
        # Tạo etl_date
        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Khởi tạo SparkSession
        spark = SparkSession.builder \
                .appName("scd_type_2_demo") \
                .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH) \
                .getOrCreate()
        
        # Tạo bảng target ở database nếu chưa tồn tại
        sql_create_target_tbl = """
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_scd2 (
            customer_id int4, 
            full_name VARCHAR(100), -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            phone_number VARCHAR(20), -- Số điện thoại 
            email VARCHAR(100),       -- Email 
            gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            start_date DATE,
            end_date DATE,
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
            CREATE TABLE IF NOT EXISTS e_commerce.customer_dim_scd2_temp (
            customer_id int4, 
            full_name VARCHAR(100), -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            phone_number VARCHAR(20), -- Số điện thoại 
            email VARCHAR(100),       -- Email 
            gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            start_date DATE,
            end_date DATE,
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
            .withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))\
            .withColumn("start_date", to_date(lit(etl_date), "yyyyMMdd"))\
            .withColumn("end_date", to_date(lit("20991231"), "yyyyMMdd"))
        
        # Thực hiện kiểm tra dữ liệu 
        print("Kiểm tra dữ liệu nguồn...")
        source_df.show(5, truncate=False)
        
        # Kiểm tra target
        print("Kiểm tra dữ liệu target:")
        target_df = spark.read\
                    .jdbc(url=JDBC_URL_POSTGRES["url"],\
                    table= "(SELECT * FROM e_commerce.customer_dim_scd2) a",
                    properties=JDBC_URL_POSTGRES["properties"])
        
        target_df.show(5, truncate=False)

        # Thực hiện thiết lập SCD Type 2
        
        ### Lọc dữ liệu insert mới
        cols = [col for col in source_df.columns if col not in ("etl_date", "start_date","end_date")]

        insert_df = source_df.select(cols).subtract(target_df.select(cols))
        insert_df = \
            insert_df\
            .withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))\
            .withColumn("start_date", to_date(lit(etl_date), "yyyyMMdd"))\
            .withColumn("end_date", to_date(lit("20991231"), "yyyyMMdd"))
        
        ### Lọc ra dữ liệu bị delete để cập nhật end_date
        update_delete_df = \
            target_df.select(cols).subtract(source_df.select(cols))
        
        update_delete_df = \
            update_delete_df.alias("ud")\
            .join(target_df.alias("t"), col("ud.customer_id") == col("t.customer_id"), "inner")\
            .select(
                "ud.*",
                to_date(lit(etl_date), "yyyyMMdd").alias("etl_date"),
                col("t.start_date"),
                to_date(lit(etl_date), "yyyyMMdd").alias("end_date")

            )
        
        ### Lọc dữ liệu không đổi
        no_change_df = \
            target_df.select(cols).alias("t")\
            .join(update_delete_df.alias("u"), col("t.customer_id") == col("u.customer_id"), "left_anti")\
            .select("t.*")
        
        no_change_df = \
            no_change_df.alias("n")\
            .join(target_df.alias("t"), col("n.customer_id") == col("t.customer_id"), "inner")\
            .select(
                "n.*",
                to_date(lit(etl_date), "yyyyMMdd").alias("etl_date"),
                col("t.start_date"),
                col("t.end_date"),
            )
        
        new_target_df = update_delete_df.union(insert_df).union(no_change_df)

        ### Kiểm tra dữ liệu target mới
        print("New Target table...")
        new_target_df.orderBy(col("customer_id").asc()).show(10, truncate=False)
        
        # Thực hiện đẩy dữ liệu mới vào bảng target theo SCD 2
        try:
            # Thực hiện đẩy dữ liệu vào bảng target temp
            new_target_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "e_commerce.customer_dim_scd2_temp") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .mode("overwrite") \
                .save()
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd2_temp.")

            # 2. Truncate dữ liệu cũ tại bảng target
            truncate_success = execute_sql_ddl (
            spark = spark,
            sql_query = "truncate table e_commerce.customer_dim_scd2",
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
            )

            # KIỂM TRA KẾT QUẢ CỦA LỆNH SQL ĐẦU TIÊN
            if not truncate_success:
                # Nếu thất bại, ném lỗi để khối except ngoài cùng bắt lấy
                raise Exception("SQL Error: Lệnh TRUNCATE thất bại.") 
            print(f"✅ TRUNCATE bảng e_commerce.customer_dim_scd2 thành công.")

            # 3. Đưa dữ liệu mới vào bảng Target
            insert_success = execute_sql_ddl (
            spark = spark,
            sql_query = "insert into e_commerce.customer_dim_scd2 select * from e_commerce.customer_dim_scd2_temp",
            jdbc_url = JDBC_URL_POSTGRES["url"],
            properties = JDBC_URL_POSTGRES["properties"]
            )

            # KIỂM TRA KẾT QUẢ CỦA LỆNH SQL THỨ HAI
            if not insert_success:
                raise Exception("SQL Error: Lệnh INSERT INTO SELECT thất bại.")
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd2.")

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
    
    parser = argparse.ArgumentParser(description='job scd_type_2_demo')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = scd_type_2_demo(etl_date=args.etl_date)
    exit(0 if success else 1)