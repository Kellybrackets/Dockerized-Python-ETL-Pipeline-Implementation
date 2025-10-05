import sys
import os
sys.path.append('/app/src')

from extract import Extractor
from transform import Transformer
from load import Loader
import time

def main():
    print("Starting ETL Pipeline...")
    start_time = time.time()
    
    try:
        # Initialize components
        extractor = Extractor()
        transformer = Transformer()
        loader = Loader()
        
        # Ensure processed table exists
        loader.create_processed_table()
        
        print("\n=== EXTRACTION PHASE ===")
        # Extract data from database
        raw_data = extractor.extract_raw_data()
        
        # Also get data from external source
        external_data = extractor.simulate_external_source()
        
        # Combine data sources
        combined_data = raw_data
        if not external_data.empty:
            combined_data = pd.concat([combined_data, external_data], ignore_index=True)
        
        print(f"Total records to process: {len(combined_data)}")
        
        if combined_data.empty:
            print("No data to process. Exiting.")
            return
        
        print("\n=== TRANSFORMATION PHASE ===")
        # Clean and transform data
        cleaned_data = transformer.clean_data(combined_data)
        transformed_data = transformer.transform_data(cleaned_data)
        
        print("\n=== LOADING PHASE ===")
        # Load transformed data
        loaded_count = loader.load_data(transformed_data)
        
        # Verify load
        print("\n=== VERIFICATION PHASE ===")
        loader.verify_load()
        
        # Generate summary
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"\n=== ETL PIPELINE COMPLETED ===")
        print(f"Execution Time: {execution_time:.2f} seconds")
        print(f"Records Processed: {transformer.processed_count}")
        print(f"Records Loaded: {loaded_count}")
        
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
