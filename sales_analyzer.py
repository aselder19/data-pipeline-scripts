# sales_analyzer.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# Set up logging and styling
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
plt.style.use('seaborn-v0_8')  # Modern plotting style

def analyze_sales_data(input_file):
    """
    Analyzes sales data and generates insights and visualizations.
    
    Args:
        input_file (str): Path to the cleaned CSV data file
    """
    try:
        # Load the cleaned data
        logger.info(f"📊 Analyzing data from: {input_file}")
        df = pd.read_csv(input_file)
        
        # Convert date column if needed
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        print("=== SALES DATA ANALYSIS ===")
        print(f"Dataset period: {df['transaction_date'].min().date()} to {df['transaction_date'].max().date()}")
        print(f"Total transactions: {len(df):,}")
        print(f"Total sales volume: ${df['sales_amount'].sum():,.2f}")
        print(f"Average transaction value: ${df['sales_amount'].mean():.2f}")
        
        # Generate key insights
        print("\n🔍 KEY INSIGHTS:")
        
        # 1. Top selling categories
        category_stats = df.groupby('product_category')['sales_amount'].agg(['sum', 'count', 'mean'])
        category_stats.columns = ['total_sales', 'transaction_count', 'avg_value']
        category_stats = category_stats.sort_values('total_sales', ascending=False)
        
        print("\n🏆 Top Product Categories by Revenue:")
        for i, (category, row) in enumerate(category_stats.head().iterrows(), 1):
            print(f"{i}. {category}: ${row['total_sales']:,.2f} ({row['transaction_count']} transactions)")
        
        # 2. Sales by state
        state_stats = df.groupby('state')['sales_amount'].sum().sort_values(ascending=False)
        print(f"\n🗺️  Top State by Sales: {state_stats.index[0]} (${state_stats.iloc[0]:,.2f})")
        
        # 3. Time-based analysis
        if 'transaction_date' in df.columns:
            daily_sales = df.groupby(df['transaction_date'].dt.date)['sales_amount'].sum()
            print(f"\n📈 Best Day: {daily_sales.idxmax()} (${daily_sales.max():,.2f})")
        
        # Create visualizations
        create_visualizations(df)
        
        logger.info("✅ Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        raise

def create_visualizations(df):
    """Creates and saves visualizations for the sales data."""
    try:
        # Create a figure with multiple subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Sales Data Analysis Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Sales by Category (Pie chart)
        category_sales = df.groupby('product_category')['sales_amount'].sum()
        ax1.pie(category_sales.values, labels=category_sales.index, autopct='%1.1f%%')
        ax1.set_title('Sales Distribution by Category')
        
        # 2. Sales by State (Bar chart)
        state_sales = df.groupby('state')['sales_amount'].sum().sort_values(ascending=False)
        ax2.bar(range(len(state_sales)), state_sales.values)
        ax2.set_title('Total Sales by State')
        ax2.set_xticks(range(len(state_sales)))
        ax2.set_xticklabels(state_sales.index, rotation=45)
        ax2.set_ylabel('Sales Amount ($)')
        
        # 3. Average Transaction Value by Category
        category_avg = df.groupby('product_category')['sales_amount'].mean().sort_values(ascending=False)
        ax3.bar(range(len(category_avg)), category_avg.values)
        ax3.set_title('Average Transaction Value by Category')
        ax3.set_xticks(range(len(category_avg)))
        ax3.set_xticklabels(category_avg.index, rotation=45)
        ax3.set_ylabel('Average Amount ($)')
        
        # 4. Daily Sales Trend (if date data is available)
        if 'transaction_date' in df.columns:
            daily_trend = df.groupby(df['transaction_date'].dt.date)['sales_amount'].sum()
            ax4.plot(daily_trend.index, daily_trend.values, marker='o')
            ax4.set_title('Daily Sales Trend')
            ax4.set_ylabel('Daily Sales ($)')
            plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('sales_analysis_dashboard.png', dpi=300, bbox_inches='tight')
        logger.info("📊 Visualizations saved as 'sales_analysis_dashboard.png'")
        
    except Exception as e:
        logger.warning(f"Could not create visualizations: {e}")

# Example usage
if __name__ == "__main__":
    print("=== Sales Data Analyzer ===")
    print("This script analyzes cleaned sales data and generates insights.\n")
    
    try:
        analyze_sales_data('cleaned_tax_data.csv')
        print("\n🎉 Analysis complete! Check 'sales_analysis_dashboard.png' for charts.")
    except FileNotFoundError:
        print("❌ Error: Run tax_data_cleaner.py first to generate cleaned data.")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
