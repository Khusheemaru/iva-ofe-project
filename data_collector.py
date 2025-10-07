import yfinance as yf
import pandas as pd
import os

#-- configuration --
# use a major F&O stock for good data
TICKER = 'RELIANCE.NS'

# using '5d' for the last 5 days and '1m' for 1-minute interval

PERIOD = '5d'
INTERVAL = '1m'

#-- ending configuration --

def fetch_stock_data(ticker, period, interval):
    """Fetches 1-minute historical data for a given ticker."""
    print(f'Fetching {interval} data for {ticker}...')
    stock_data = yf.download(tickers=ticker, period=period, interval=interval, auto_adjust=True)

    if stock_data.empty:
        print(f'No data found for {ticker}. It might be delisted or invalid ticker.')
        return None
    
    print('Data fetched successfully.')
    return stock_data

def save_data(df, ticker):
    """Saves the DataFrame to a CSV file."""

    if not os.path.exists('data'):
        os.makedirs('data')

    filename = f"data/{ticker.replace('.NS','')}_1m_data.csv"

    df.to_csv(filename)
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    # Fetch the data
    data = fetch_stock_data(TICKER, PERIOD, INTERVAL)
    

    if data is not None:
        save_data(data, TICKER)
        print("\nFirst 5 rows of data:")
        print(data.head())
