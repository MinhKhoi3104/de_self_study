import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)

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