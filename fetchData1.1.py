import time
import pandas as pd
import requests
import io
import logging

# Setup logging
logging.basicConfig(filename='/fetchData_errors.log', level=logging.ERROR, # Path to save error logs to
                    format='%(asctime)s:%(levelname)s:%(message)s')

def fetch_data_month(symbol, api_key, year, month):
    formatted_month = f"{year}-{month:02d}"
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=15min&month={formatted_month}&apikey={api_key}&outputsize=full&datatype=csv&adjusted=true&extended_hours=true"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return pd.read_csv(io.BytesIO(response.content))
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - For month: {formatted_month}")
    except Exception as err:
        logging.error(f"Other error occurred: {err} - For month: {formatted_month}")
    return pd.DataFrame()

def main():
    symbol = "QQQ" # Stock/ETF symbol
    api_key = ""  # Replace with your actual API key
    all_data = pd.DataFrame()
    
    # Limiting the data request to the past two years
    current_year = pd.Timestamp.now().year
    for year in range(current_year - 1, current_year + 1):  # 2 years
        for month in range(1, 13):  # 12 months
            data_month = fetch_data_month(symbol, api_key, year, month)
            if not data_month.empty:
                all_data = pd.concat([all_data, data_month], ignore_index=True)
            time.sleep(60)  # sleep to respect API rate limits

    # Save the concatenated dataframe to a CSV file
    csv_file_path = '/Data/QQQ_2_years_intraday.csv' # Path to save data to
    all_data.to_csv(csv_file_path, index=False)
    return csv_file_path

if __name__ == "__main__":
    csv_file_path = main()
    print(f"Data saved to {csv_file_path}")
