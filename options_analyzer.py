from nsepython import nse_optionchain_scrapper
import pandas as pd

def fetch_option_chain(ticker):
    """fetches live option chain """
    print(f"\n Fetching option chain for {ticker}...")
    try:
        option_data = nse_optionchain_scrapper(ticker)
        if option_data:
            print("options chain fetched successfully.")
            return option_data
        
        else:
            print(f"Could not fetch options data for {ticker}.")
            return None
    except Exception as e:
        print(f"an error occured while fetching options data: {e}")
        return None
    
if __name__ == '__main__':
    ticker_symbol = 'RELIANCE'
    options_json = fetch_option_chain(ticker_symbol)

    if options_json:

        #fetch_option_chain returns data in dict form
        #we convert list of dict to pandas dataframe

        df = pd.DataFrame(options_json['records']['data'])
        print("\nSuccessfully converted options to dataframe.")
        print("columns available: ", df.columns.tolist())
        print("\n first 5 rows of the options chain: ")

        print(df[['strikePrice', 'expiryDate', 'CE', 'PE']].head())