# main.py (Upgraded with Looping and Scoring)

from anomaly_detector import load_data, find_volume_anomalies
from options_analyzer import fetch_option_chain
import pandas as pd
import time

# -- Configuration --
TICKER_SYMBOL = 'RELIANCE'
# -- End Configuration --

def analyze_options_flow(options_data):
    """
    Analyzes options data and returns a Confirmation Score (0-5).
    A higher score indicates a stronger institutional footprint.
    """
    try:
        df_options = pd.DataFrame(options_data['records']['data'])
        
        # --- 1. Data Preparation ---
        def extract_option_details(option_data):
            if not isinstance(option_data, dict):
                return {'volume': 0, 'ltp': 0, 'bid': 0, 'ask': 0, 'oi': 0}
            return {
                'volume': option_data.get('totalTradedVolume', 0),
                'ltp': option_data.get('lastPrice', 0),
                'bid': option_data.get('bidprice', 0),
                'ask': option_data.get('askPrice', 0),
                'oi': option_data.get('openInterest', 0)
            }

        ce_details = df_options['CE'].apply(extract_option_details).apply(pd.Series)
        pe_details = df_options['PE'].apply(extract_option_details).apply(pd.Series)
        
        ce_details['strikePrice'] = df_options['strikePrice']
        pe_details['strikePrice'] = df_options['strikePrice']

        # --- 2. The Scoring Logic ---
        confirmation_score = 0
        reason = "No significant options activity found."

        total_ce_volume = ce_details['volume'].sum()
        total_pe_volume = pe_details['volume'].sum()
        
        # Score +1: Is there significant overall options volume?
        if (total_ce_volume + total_pe_volume) > 10000: # Threshold can be adjusted
            confirmation_score += 1
            reason = "Significant overall options volume."

        # Score +2: Is there a strong directional bias? (e.g., Call volume is 2x Put volume)
        if total_ce_volume > total_pe_volume * 2:
            confirmation_score += 2
            top_contract = ce_details.loc[ce_details['volume'].idxmax()]
            reason = f"Strong bullish bias: Call volume ({total_ce_volume}) is >2x Put volume ({total_pe_volume})."
        elif total_pe_volume > total_ce_volume * 2:
            confirmation_score += 2
            top_contract = pe_details.loc[pe_details['volume'].idxmax()]
            reason = f"Strong bearish bias: Put volume ({total_pe_volume}) is >2x Call volume ({total_ce_volume})."
        else:
            return confirmation_score, reason # Exit if no clear direction

        # Score +2: Was the most active contract traded near its mid-price? ("Smart Money" check)
        mid_price = (top_contract['bid'] + top_contract['ask']) / 2
        if mid_price > 0 and abs(top_contract['ltp'] - mid_price) < 0.05 * mid_price: # within 5% of mid-price
            confirmation_score += 2
            reason += f" High activity at strike {top_contract['strikePrice']} occurred near its mid-price, indicating institutional flow."
            
        return confirmation_score, reason

    except Exception as e:
        return 0, f"Error during options analysis: {e}"


if __name__ == "__main__":
    print("--- Starting IVA-OFE Engine ---")
    
    stock_df = load_data(TICKER_SYMBOL)
    if stock_df is not None:
        volume_anomalies = find_volume_anomalies(stock_df)
        
        if not volume_anomalies.empty:
            print(f"\nFound {len(volume_anomalies)} anomalies. Analyzing each...")
            
            # This list will store the results of our analysis for each anomaly
            analysis_results = []
            
            # --- The New Loop ---
            for timestamp, anomaly_data in volume_anomalies.iterrows():
                print(f"\n-> Analyzing anomaly at {timestamp}...")
                
                # Fetch fresh options data for each analysis
                options_data = fetch_option_chain(TICKER_SYMBOL)
                
                if options_data:
                    score, reason = analyze_options_flow(options_data)
                    print(f"Confirmation Score: {score}/5. Reason: {reason}")
                    analysis_results.append({
                        'timestamp': timestamp,
                        'z_score': anomaly_data['volume_z_score'],
                        'score': score,
                        'reason': reason
                    })
                    # Pause to avoid overwhelming the NSE server
                    time.sleep(1) 
                else:
                    print("Could not fetch options data for this anomaly.")

            # --- Find the Best Signal ---
            if analysis_results:
                best_signal = max(analysis_results, key=lambda x: x['score'])
                
                print("\n--- IVA-OFE Engine Run Complete ---")
                if best_signal['score'] > 2: # Set a threshold for a "high conviction" signal
                    print(f"\n🔥🔥🔥 STRONGEST SIGNAL OF THE DAY 🔥🔥🔥")
                    print(f"Timestamp: {best_signal['timestamp']}")
                    print(f"Stock Volume Z-Score: {best_signal['z_score']:.2f}")
                    print(f"Options Confirmation Score: {best_signal['score']}/5")
                    print(f"Reason: {best_signal['reason']}")
                else:
                    print("\nNo high-conviction signals found after analysis.")
            else:
                print("\nAnalysis complete, but no results were generated.")
        else:
            print("\nNo significant stock volume anomalies found.")