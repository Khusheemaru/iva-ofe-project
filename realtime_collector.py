# realtime_collector.py

import pandas as pd
import yfinance as yf
from nsepython import nse_optionchain_scrapper
import time
import os
from datetime import datetime

# -- Configuration --
TICKER_SYMBOL = 'RELIANCE'
OUTPUT_FILE = f"data/{TICKER_SYMBOL}_data_{datetime.now().strftime('%Y-%m-%d')}.csv"
# -- End Configuration --

def get_live_stock_data(ticker):
    """Fetches the latest available stock data point."""
    # Fetch 1 day of 1m data and return the very last row
    data = yf.download(tickers=f"{ticker}.NS", period='1d', interval='1m', auto_adjust=True)
    return data.iloc[-1] if not data.empty else None

def get_live_options_data(ticker):
    """Fetches the full live options chain."""
    try:
        return nse_optionchain_scrapper(ticker)
    except Exception:
        return None

if __name__ == "__main__":
    print("--- Starting Real-Time Data Collector ---")
    print(f"Saving data to: {OUTPUT_FILE}")

    # Create the header of the CSV file if it doesn't exist
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'w') as f:
            f.write("timestamp,stock_price,stock_volume,total_ce_volume,total_pe_volume\n")

    # This loop will run until you manually stop it (Ctrl+C)
    try:
        while True:
            # Get current time
            now = datetime.now()
            
            # --- Only run during approximate market hours (in IST) ---
            if now.hour >= 9 and now.hour <= 15:
                # --- 1. Fetch Stock Data ---
                stock_data = get_live_stock_data(TICKER_SYMBOL)
                
                # --- 2. Fetch Options Data ---
                options_data_raw = get_live_options_data(TICKER_SYMBOL)

                if stock_data is not None and options_data_raw:
                    # --- 3. Process & Combine Data ---
                    df_options = pd.DataFrame(options_data_raw['records']['data'])
                    
                    # Calculate total call and put volumes
                    ce_volume = df_options['CE'].apply(lambda x: x.get('totalTradedVolume', 0) if isinstance(x, dict) else 0).sum()
                    pe_volume = df_options['PE'].apply(lambda x: x.get('totalTradedVolume', 0) if isinstance(x, dict) else 0).sum()
                    
                    # Create a single row of data
                    log_entry = (
                        f"{now},"
                        f"{stock_data['Close']},"
                        f"{stock_data['Volume']},"
                        f"{ce_volume},"
                        f"{pe_volume}\n"
                    )
                    
                    # --- 4. Save to CSV ---
                    with open(OUTPUT_FILE, 'a') as f:
                        f.write(log_entry)
                        
                    print(f"[{now.strftime('%H:%M:%S')}] Logged data point. Stock Vol: {stock_data['Volume']}, Call Vol: {ce_volume}")
                else:
                    print(f"[{now.strftime('%H:%M:%S')}] Could not fetch complete data. Retrying...")

                # --- 5. Wait for the next minute ---
                time.sleep(60)
            else:
                print(f"Market is closed. Collector is sleeping. Current time: {now.strftime('%H:%M:%S')}")
                time.sleep(300) # Sleep for 5 minutes outside market hours

    except KeyboardInterrupt:
        print("\n--- Collector stopped manually. ---")