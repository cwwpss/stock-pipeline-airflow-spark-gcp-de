from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

def main():
    current_date = datetime.now().strftime("%Y-%m-%d")
    gcs_file_name_fund = f"{current_date}_stock_fundamentals.csv"
    
    spark = SparkSession.builder.appName("prep data").getOrCreate()
    
    # 1. Read the CSV data (make sure the file path is correct)
    df_fund = spark.read.csv(f"gs://de-cwwps-newyork-stock-project/raw_data/{gcs_file_name_fund}", header=True, inferSchema=True)
    df_sec = spark.read.csv("./include/securities.csv", header=True, inferSchema=True)
    
    # Select fund columns
    fund_selected = [
    "Ticker Symbol", "Period Ending", "Total Revenue", "Gross Profit",
    "Total Liabilities", "Long-Term Debt"
    ]
    sec_selected = ["Ticker symbol", "GICS Sector", "GICS Sub Industry"]
    df_fund = df_fund.select(*fund_selected)
    df_sec = df_sec.select(*sec_selected)

    # Rename columns
    df_fund = df_fund.withColumnRenamed("Ticker Symbol", "Ticker_symbol_fund")
    df_sec = df_sec.withColumnRenamed("Ticker Symbol", "Ticker_symbol_sec")
    
    # Check null
    df_fund.summary("count").show()

    print("==schema==")
    print("df fund\n")
    df_fund.printSchema()

    # Merge data
    df_fund_merged = df_fund.join(df_sec, df_fund["Ticker_symbol_fund"] == df_sec["Ticker_symbol_sec"], "left")
    
    df_fund_merged = df_fund_merged.withColumn("Period Ending", F.to_date("Period Ending", "yyyy-MM-dd"))

    fund_merged_selectd = [
        "Ticker_Symbol_fund", "Period Ending", "Total Revenue", "Gross Profit",
        "Total Liabilities", "Long-Term Debt", "GICS Sector", "GICS Sub Industry"
    ]

    df_fund_merged = df_fund_merged.select(*fund_merged_selectd)

    # Rename final df
    rename_col = {
    "Ticker_Symbol_fund": "ticker_symbol_fund",
    "Period Ending": "period_ending",
    "Total Revenue": "total_revenue",
    "Gross Profit": "gross_profit",
    "Total Liabilities": "total_liabilities",
    "Long-Term Debt": "long_term_debt",
    "GICS Sector": "gics_sector",
    "GICS Sub Industry": "gics_sub_industry"
    }

    df_fund_merged = df_fund_merged.select([F.col(col).alias(rename_col.get(col, col)) for col in df_fund_merged.columns])

    file_path = f"gs://de-cwwps-newyork-stock-project/prep_data/{current_date}_stock_fund_prep.parquet"
    
    df_fund_merged.write.mode("overwrite").parquet(file_path)

    df_fund_merged.show()

    # 6. Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()