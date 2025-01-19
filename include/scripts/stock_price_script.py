from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

def main():
    current_date = datetime.now().strftime("%Y-%m-%d")
    gcs_file_name_price = f"{current_date}_stock_prices.csv"
    spark = SparkSession.builder.appName("prep data").getOrCreate()
    
    # 1. Read the CSV data (make sure the file path is correct)
    df_price = spark.read.csv(f"gs://de-cwwps-newyork-stock-project/raw_data/{gcs_file_name_price}", header=True, inferSchema=True)
    df_sec = spark.read.csv("./include/securities.csv", header=True, inferSchema=True)
    df_exchange = spark.read.csv(f"gs://de-cwwps-newyork-stock-project/{current_date}_currency_exchange.csv", header=True, inferSchema=True)

    # Select columns
    prices_selected = ["date", "symbol","open", "close", "low", "high", "volume"]
    sec_selected = ["Ticker symbol", "GICS Sector", "GICS Sub Industry"]
    exchange_selected = ["exchange_date", "exchange_rate"]
    df_sec = df_sec.select(*sec_selected)
    df_price = df_price.select(*prices_selected)
    df_exchange = df_exchange.select(*exchange_selected)

    # Rename columns
    df_sec = df_sec.withColumnRenamed("Ticker Symbol", "Ticker_symbol_sec")

    print("==schema==")
    df_price.printSchema()

    # Merge data
    df_price_merged = df_price.join(df_sec, df_price["symbol"] == df_sec["Ticker_symbol_sec"], "left")
    
    # Clean date
    df_price_merged = df_price_merged.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

    price_merged_selectd = [
        "date", "symbol", "open", "close", "low", "high", "volume", "GICS Sector", "GICS Sub Industry"
    ]

    df_price_merged = df_price_merged.select(*price_merged_selectd)

    # Check null
    df_price_merged.summary("count").show()

    # Calculate Other metrics in stock prices

    # Difine window for partion data and order
    window_sma10 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-9, 0)
    window_sma50 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-49, 0)

    # SMA 10
    df_price_merged = df_price_merged.withColumn("sma10_step1", F.avg("close").over(window_sma10))\
        .withColumn("row_count", F.count("close").over(window_sma10))\
        .withColumn("sma10", 
                    F.when(F.col("row_count") >= 10, F.col("sma10_step1")).otherwise(None))
    df_price_merged = df_price_merged.drop("sma10_step1", "row_count")

    # SMA 50
    df_price_merged = df_price_merged.withColumn("sma50_step1", F.avg("close").over(window_sma50))\
        .withColumn("row_count", F.count("close").over(window_sma50))\
        .withColumn("sma50", 
                    F.when(F.col("row_count") >= 50, F.col("sma50_step1")).otherwise(None))
    df_price_merged = df_price_merged.drop("sma50_step1", "row_count")

    # RSI
    window_rsi = Window.partitionBy("symbol").orderBy("date")
    window_rsi14 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)

    # Calculate price changes, gain and loss
    df_price_merged = df_price_merged.withColumn("price_change", F.col("close") - F.lag("close", 1).over(window_rsi))

    df_price_merged = df_price_merged.withColumn('gain', 
                    F.when(F.col('price_change')>0, F.col('price_change')).otherwise(0)) \
                    .withColumn('loss', 
                    F.when(F.col('price_change')<0, -F.col('price_change')).otherwise(0))

    # Calculate RSI
    df_price_merged = df_price_merged.withColumn('avg_gain', F.avg("gain").over(window_rsi14)) \
                    .withColumn('avg_loss', F.avg('loss').over(window_rsi14))\
                    .withColumn('rsi_step1', 100 - (100/(1 + F.col('avg_gain')/F.col('avg_loss'))))

    df_price_merged = df_price_merged.withColumn("rsi_step1", F.avg("close").over(window_rsi14))\
        .withColumn("row_count", F.count("close").over(window_rsi14))\
        .withColumn("rsi", 
                    F.when(F.col("row_count") >= 14, F.col("rsi_step1")).otherwise(None))
    
    df_price_merged = df_price_merged.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rsi_step1", "row_count")

    # Add THB exchange rate and convert USD to THB
    df_price_merged = df_price_merged.withColumn("p_year", F.year(F.col("date"))) \
                                   .withColumn("p_month", F.month(F.col("date")))
    
    df_exchange = df_exchange.withColumn("e_year", F.year(F.col("exchange_date"))) \
                                   .withColumn("e_month", F.month(F.col("exchange_date")))
    
    df_price_merged = df_price_merged.join(
    df_exchange,
    (df_price_merged["p_year"] == df_exchange["e_year"]) & (df_price_merged["p_month"] == df_exchange["e_month"]),
    "left"
    )

    df_price_merged = df_price_merged.withColumn("close_thb", F.col("exchange_rate") * F.col("close"))
    
    df_price_merged = df_price_merged.drop("exchange_date", "p_year", "p_month","e_year", "e_month", "exchange_rate")

    # Rename final df
    rename_col = {
    "date": "date",
    "symbol": "symbol",
    "open": "open_price",
    "close": "close_price",
    "low": "low_price",
    "high": "high_price",
    "volume": "volume",
    "GICS Sector": "gics_sector",
    "GICS Sub Industry": "gics_sub_industry",
    "sma10": "sma10",
    "sma50": "sma50",
    "rsi": "rsi",
    "close_thb": "close_price_thb"
    }

    df_price_merged = df_price_merged.select([F.col(c).alias(rename_col[c]) for c in df_price_merged.columns])

    file_path = f"gs://de-cwwps-newyork-stock-project/prep_data/{current_date}_stock_price_prep.parquet"
    
    df_price_merged.write.mode("overwrite").parquet(file_path)
    df_price_merged.show(100)

    # 6. Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()