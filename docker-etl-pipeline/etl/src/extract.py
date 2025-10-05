import psycopg2
import os
import pandas as pd
from datetime import datetime

class Extractor:
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
    
    def extract_raw_data(self):
        """Extract raw data from database"""
        try:
            conn = self.get_connection()
            query = """
                SELECT transaction_id, customer_id, product_name, category, 
                       amount, transaction_date, region
                FROM sales_data
                WHERE processed_amount IS NULL
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            print(f"Extracted {len(df)} records from database")
            return df
            
        except Exception as e:
            print(f"Error during extraction: {str(e)}")
            return pd.DataFrame()
    
    def simulate_external_source(self):
        """Simulate data from external source (API, CSV, etc.)"""
        external_data = [
            {
                'transaction_id': 'TXN006',
                'customer_id': 1006,
                'product_name': 'Wireless Mouse',
                'category': 'Electronics',
                'amount': 45.99,
                'transaction_date': '2024-01-20',
                'region': 'South'
            },
            {
                'transaction_id': 'TXN007',
                'customer_id': 1007,
                'product_name': 'Office Desk',
                'category': 'Furniture',
                'amount': 450.00,
                'transaction_date': '2024-01-21',
                'region': 'North'
            },
            {
                'transaction_id': 'TXN008',
                'customer_id': 1008,
                'product_name': 'Stapler',
                'category': 'Stationery',
                'amount': 8.50,
                'transaction_date': '2024-01-22',
                'region': 'East'
            }
        ]
        
        return pd.DataFrame(external_data)
