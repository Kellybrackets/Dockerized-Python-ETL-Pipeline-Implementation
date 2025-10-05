import pandas as pd
import numpy as np
from datetime import datetime

class Transformer:
    def __init__(self):
        self.processed_count = 0
    
    def clean_data(self, df):
        """Clean and validate the extracted data"""
        if df.empty:
            return df
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['transaction_id'])
        
        # Handle missing values
        df['amount'] = df['amount'].fillna(0)
        df['category'] = df['category'].fillna('Unknown')
        df['region'] = df['region'].fillna('Unknown')
        
        # Validate amount
        df = df[df['amount'] >= 0]
        
        print(f"Data cleaned. {len(df)} valid records remaining")
        return df
    
    def transform_data(self, df):
        """Apply business transformations"""
        if df.empty:
            return df
        
        # Convert date column
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Calculate processed amount with tax
        df['processed_amount'] = df['amount'] * 1.08  # Adding 8% tax
        
        # Categorize transaction size
        df['transaction_size'] = np.where(
            df['amount'] > 500, 'Large',
            np.where(df['amount'] > 100, 'Medium', 'Small')
        )
        
        # Create customer segment based on transaction history
        df['customer_segment'] = np.where(
            df['amount'] > 800, 'Premium',
            np.where(df['amount'] > 300, 'Standard', 'Basic')
        )
        
        # Add processing timestamp
        df['processed_timestamp'] = datetime.now()
        
        self.processed_count = len(df)
        print(f"Transformed {self.processed_count} records")
        
        return df
    
    def get_transformation_summary(self):
        """Get summary of transformation process"""
        return {
            'processed_records': self.processed_count,
            'timestamp': datetime.now()
        }
