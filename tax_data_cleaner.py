# tax_data_cleaner.py
import pandas as pd
import numpy as np
import logging

# Set up logging to track the pipeline process
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_sales_data(input_file, output_file):
    """
    Cleans and transforms raw sales data for tax reporting.
    Demonstrates ETL (Extract, Transform, Load) processes for financial data.
    
    Args:
        input_file (str): Path to the raw CSV data file
        output_file (str): Path to save the cleaned CSV data
    
    Returns:
        pandas.DataFrame: The cleaned and transformed DataFrame
    """
    try:
        # EXTRACT: Read data from a CSV file
        logger.info(f"📂 Reading data from: {input_file}")
        df = pd.read_csv(input_file)
        logger.info(f"✅ Data loaded successfully. Shape: {df.shape}")
        
        # Display original data sample
        print("\n=== ORIGINAL DATA SAMPLE ===")
        print(df.head(3))
        
        # TRANSFORM: Clean and process the data
        logger.info("🔄 Transforming data...")
        
        # 1. Handle missing values
        df['sales_amount'].fillna(0, inplace=True)
        df['state'].fillna('UNKNOWN', inplace=True)
        
        # 2. Remove duplicate records
        initial_count = len(df)
        df.drop_duplicates(inplace=True)
        duplicates_removed = initial_count - len(df)
        logger.info(f"🧹 Removed {duplicates_removed} duplicate records")
        
        # 3. Filter out negative sales (returns should be handled separately)
        df = df[df['sales_amount'] >= 0]
        
        # 4. Convert date column to datetime format with error handling
        try:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
            # Remove rows where date conversion failed
            df = df.dropna(subset=['transaction_date'])
        except Exception as e:
            logger.warning(f"Date conversion issues: {e}")
        
        # 5. Create new features for analysis
        df['transaction_month'] = df['transaction_date'].dt.to_period('M')
        df['transaction_quarter'] = df['transaction_date'].dt.quarter
        
        # 6. Categorize products for tax reporting
        def categorize_product(product_id):
            if pd.isna(product_id):
                return 'UNCATEGORIZED'
            product_str = str(product_id).upper()
            if product_str.startswith('ELE'):
                return 'ELECTRONICS'
            elif product_str.startswith('CLO'):
                return 'CLOTHING'
            elif product_str.startswith('GRO'):
                return 'GROCERIES'
            else:
                return 'OTHER'
        
        df['product_category'] = df['product_id'].apply(categorize_product)
        
        # 7. Add tax jurisdiction based on state and product type
        def determine_tax_jurisdiction(state, category):
            if state in ['CA', 'NY', 'TX'] and category != 'GROCERIES':
                return 'STANDARD'
            elif category == 'GROCERIES':
                return 'GROCERY_EXEMPT'
            else:
                return 'REDUCED_RATE'
        
        df['tax_jurisdiction'] = df.apply(
            lambda row: determine_tax_jurisdiction(row['state'], row['product_category']), 
            axis=1
        )
        
        # LOAD: Save the cleaned and transformed data
        df.to_csv(output_file, index=False)
        logger.info(f"💾 Cleaned data saved to: {output_file}")
        
        # Generate and display a summary report
        print("\n=== DATA CLEANING SUMMARY ===")
        print(f"Final dataset shape: {df.shape}")
        print(f"Total sales amount: ${df['sales_amount'].sum():,.2f}")
        
        # Summary by state
        state_summary = df.groupby('state')['sales_amount'].agg(['sum', 'count'])
        print("\n📊 Sales by State:")
        print(state_summary.head())
        
        # Summary by product category
        category_summary = df.groupby('product_category')['sales_amount'].agg(['sum', 'count'])
        print("\n📦 Sales by Category:")
        print(category_summary)
        
        return df
        
    except FileNotFoundError:
        logger.error("❌ Error: The input file was not found.")
        raise
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {str(e)}")
        raise

def generate_sample_data():
    """
    Generates sample sales data for demonstration purposes.
    This simulates the raw data that would come from a source system.
    """
    sample_data = {
        'transaction_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007],
        'transaction_date': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-18', '2024-01-19'],
        'product_id': ['ELE-1001', 'CLO-2001', 'ELE-1002', 'GRO-3001', 'ELE-1001', 'OTHER-999', 'ELE-1003'],
        'product_name': ['Smartphone', 'T-Shirt', 'Laptop', 'Apples', 'Smartphone', 'Misc Item', 'Tablet'],
        'sales_amount': [799.99, 29.99, 1299.99, 5.99, 799.99, 15.50, 459.99],
        'state': ['CA', 'NY', 'TX', 'CA', 'AZ', 'NY', 'CA'],
        'customer_id': [201, 202, 203, 204, 205, 206, 207]
    }
    
    sample_df = pd.DataFrame(sample_data)
    sample_df.to_csv('sample_sales_data.csv', index=False)
    logger.info("📝 Sample data generated: sample_sales_data.csv")
    return sample_df

# Example usage and demonstration
if __name__ == "__main__":
    print("=== Sales Data Cleaning Pipeline ===")
    print("This script demonstrates ETL processes for financial data transformation.\n")
    
    # Generate sample data for demonstration
    generate_sample_data()
    
    # Run the cleaning pipeline on the sample data
    try:
        cleaned_data = clean_sales_data('sample_sales_data.csv', 'cleaned_tax_data.csv')
        
        print("\n🎉 Pipeline completed successfully!")
        print("\n=== CLEANED DATA SAMPLE ===")
        print(cleaned_data[['transaction_date', 'product_category', 'state', 'sales_amount', 'tax_jurisdiction']].head())
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
