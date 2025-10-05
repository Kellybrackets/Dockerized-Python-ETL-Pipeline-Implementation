-- Create target table for ETL data
CREATE TABLE IF NOT EXISTS sales_data (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    customer_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    transaction_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    processed_amount DECIMAL(10,2),
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_transaction_date ON sales_data(transaction_date);
CREATE INDEX IF NOT EXISTS idx_customer_id ON sales_data(customer_id);

-- Insert some sample raw data (simulating source data)
INSERT INTO sales_data (transaction_id, customer_id, product_name, category, amount, transaction_date, region) VALUES
    ('TXN001', 1001, 'Laptop', 'Electronics', 1200.00, '2024-01-15', 'North'),
    ('TXN002', 1002, 'Desk Chair', 'Furniture', 250.50, '2024-01-16', 'South'),
    ('TXN003', 1003, 'Smartphone', 'Electronics', 800.00, '2024-01-17', 'East'),
    ('TXN004', 1004, 'Notebook', 'Stationery', 15.99, '2024-01-18', 'West'),
    ('TXN005', 1005, 'Monitor', 'Electronics', 300.75, '2024-01-19', 'North')
ON CONFLICT (transaction_id) DO NOTHING;
