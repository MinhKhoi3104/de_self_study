import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from config.database_config import JDBC_URL_POSTGRES,PG_JDBC_JAR_PATH

def scd_type_1_demo(etl_date=None):
    try:
        # Tạo etl_date
        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Khởi tạo SparkSession
        spark = SparkSession.builder \
                .appName("scd_type_1_demo") \
                .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH) \
                .getOrCreate()
        
        # Thực hiện gọi lấy dữ liệu bảng nguồn
        source_df = spark.read\
                    .jdbc(url=JDBC_URL_POSTGRES["url"],\
                    table= "(SELECT * FROM e_commerce.customer) a",
                    properties=JDBC_URL_POSTGRES["properties"])
        
        # Thực hiện kiểm tra dữ liệu 
        source_df.show(5, truncate=False)

        # Thêm cột etl_date vào tbl
        source_df = source_df.withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))

        # Kiểm tra target
        print("Kiểm tra dữ liệu target trước khi import vào:")
        source_df.show(5, truncate=False)

        # Thực hiện đẩy dữ liệu mới vào bảng target theo SCD 1
        try:
            source_df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "e_commerce.customer_dim_scd1") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
            print(f"✅ Ghi dữ liệu thành công vào bảng e_commerce.customer_dim_scd1.")

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
    
    parser = argparse.ArgumentParser(description='job scd_type_1_demo')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = scd_type_1_demo(etl_date=args.etl_date)
    exit(0 if success else 1)
    
