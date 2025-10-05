import psycopg2
import os
import pandas as pd
from datetime import datetime

class Loader:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'database'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'etl_db'),
            'user': os.getenv('DB_USER', 'etl_user'),
            'password': os.getenv('DB_PASSWORD', 'etl_password')
        }
    
    def get_connection(self):
        """Create database connection"""
        return psycopg2.connect(**self.db_config)
    
    def create_processed_table(self):
        """Create table for processed data if it doesn't exist"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            create_table_query = """
                CREATE TABLE IF NOT EXISTS processed_sales_data (
                    id SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(50) UNIQUE NOT NULL,
                    customer_id INTEGER NOT NULL,
                    product_name VARCHAR(100) NOT NULL,
                    category VARCHAR(50) NOT NULL,
                    original_amount DECIMAL(10,2) NOT NULL,
                    processed_amount DECIMAL(10,2) NOT NULL,
                    transaction_date DATE NOT NULL,
                    region VARCHAR(50) NOT NULL,
                    transaction_size VARCHAR(20) NOT NULL,
                    customer_segment VARCHAR(20) NOT NULL,
                    processed_timestamp TIMESTAMP NOT NULL,
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
            conn.close()
            
            print("Processed table created/verified")
            
        except Exception as e:
            print(f"Error creating processed table: {str(e)}")
    
    def load_data(self, df):
        """Load transformed data into database"""
        if df.empty:
            print("No data to load")
            return 0
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Update original table with processed amount
            update_query = """
                UPDATE sales_data 
                SET processed_amount = %s
                WHERE transaction_id = %s
            """
            
            for _, row in df.iterrows():
                cursor.execute(update_query, (row['processed_amount'], row['transaction_id']))
            
            # Insert into processed table
            insert_query = """
                INSERT INTO processed_sales_data 
                (transaction_id, customer_id, product_name, category, original_amount,
                 processed_amount, transaction_date, region, transaction_size, 
                 customer_segment, processed_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    processed_amount = EXCLUDED.processed_amount,
                    transaction_size = EXCLUDED.transaction_size,
                    customer_segment = EXCLUDED.customer_segment,
                    load_timestamp = CURRENT_TIMESTAMP
            """
            
            for _, row in df.iterrows():
                cursor.execute(insert_query, (
                    row['transaction_id'], row['customer_id'], row['product_name'],
                    row['category'], row['amount'], row['processed_amount'],
                    row['transaction_date'], row['region'], row['transaction_size'],
                    row['customer_segment'], row['processed_timestamp']
                ))
            
            conn.commit()
            loaded_count = len(df)
            cursor.close()
            conn.close()
            
            print(f"Successfully loaded {loaded_count} records")
            return loaded_count
            
        except Exception as e:
            print(f"Error during loading: {str(e)}")
            return 0
    
    def verify_load(self):
        """Verify that data was loaded correctly"""
        try:
            conn = self.get_connection()
            
            # Check counts
            count_query = "SELECT COUNT(*) FROM processed_sales_data"
            df_count = pd.read_sql_query(count_query, conn)
            
            # Get sample of loaded data
            sample_query = """
                SELECT transaction_id, product_name, original_amount, processed_amount,
                       transaction_size, customer_segment
                FROM processed_sales_data 
                ORDER BY load_timestamp DESC 
                LIMIT 5
            """
            df_sample = pd.read_sql_query(sample_query, conn)
            
            conn.close()
            
            print(f"Total records in processed table: {df_count.iloc[0,0]}")
            print("Sample of loaded data:")
            print(df_sample)
            
            return True
            
        except Exception as e:
            print(f"Error verifying load: {str(e)}")
            return False
