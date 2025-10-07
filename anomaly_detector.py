import pandas as pd

#-- configuration --
TICKER_SYMBOL =  'RELIANCE'
ROLLING_WINDOW = 30
Z_SCORE_THRESHOLD = 4.0
#-- end configuration --

def load_data(ticker):
    """Loads the previously saved stock data."""
    filename = f"data/{ticker}_1m_data.csv"
    try:
        df = pd.read_csv(filename, index_col =0, parse_dates=True, skiprows=[1, 2])
        print(f"Loaded data for {ticker}.")
        return df
    except FileNotFoundError:
        print(f"Error: Data file not found at {filename}")
        return None
    
def find_volume_anomalies(df):
    """Calculate volume z-score and find anomalies"""
    # Calculate rolling average and standard deviation
    df['volume_rolling_avg'] = df['Volume'].rolling(window = ROLLING_WINDOW).mean()
    df['volume_rolling_std'] = df['Volume'].rolling(window = ROLLING_WINDOW).std()

    # Calculate the Z-score
    df['volume_z_score'] = (df['Volume'] - df['volume_rolling_avg']) / df['volume_rolling_std']

    # Find anomalies
    anomalies = df[df['volume_z_score'] > Z_SCORE_THRESHOLD]

    print(f"Found {len(anomalies)} potential anomalies.")
    return anomalies

if __name__ == "__main__":
    #load data
    stock_df = load_data(TICKER_SYMBOL)

    if stock_df is not None:
        #find anomalies
        volume_anomalies = find_volume_anomalies(stock_df)

        if not volume_anomalies.empty:
            print("\n--- High Volume Anomalies Detected ---")
            #print the detected anomalies
            print(volume_anomalies[['Volume', 'volume_z_score']])
        else:
            print("\nNo significant volume anomalies found with the current threshold.")