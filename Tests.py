import requests
from datetime import datetime
import time
import pandas as pd
import os

# Importing SP500 tickers

#------------------------------------------------------------------------

sp500 = pd.read_csv('sp500_04-05-2024.csv', header=0, index_col=0)
sp500_tickers = sp500['ticker']
tkrs_to_do = ['CAT', 'NVDA']
#sp500_tickers = [i for i in sp500_tickers if i not in complex_tkrs]

#------------------------------------------------------------------------

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

def fetch_comments(ticker, start_date, end_date, max_retries=5):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
    comments = []
    max_id = None

    while True:
        params = {'max': max_id} if max_id else {}
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for {ticker} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return comments, False

        data = response.json()
        if 'messages' not in data:
            print(f"No messages found for {ticker}")
            break

        for message in data['messages']:
            created_at = datetime.strptime(message['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            if created_at < start_date:
                return comments, True
            if start_date <= created_at <= end_date:
                comments.append({
                    'ticker': ticker,
                    'id': message['id'],
                    'created_at': created_at,
                    'user': message['user']['username'],
                    'body': message['body']
                })

        if 'messages' in data and data['messages']:
            max_id = data['messages'][-1]['id'] - 1
        else:
            break

        time.sleep(1)

    return comments, True

# Define date range
start_date = datetime(2024, 3, 1)
end_date = datetime(2024, 3, 15)

#all_comments = pd.read_csv("stocktwits_data.csv")
new_twits_df = pd.DataFrame()
error_tickers = []

for ticker in tkrs_to_do:
    print(f"Fetching comments for {ticker}")
    comments, success = fetch_comments(ticker, start_date, end_date)
    if success:
        comments_df = pd.DataFrame(comments)
        new_twits_df = pd.concat([new_twits_df, comments_df], ignore_index=True)
        #all_comments = pd.concat([all_comments, comments_df], ignore_index=True)
    else:
        error_tickers.append(ticker)

#all_comments.to_csv("stocktwits_data_new.csv", index=False, escapechar='\\')
new_twits_df.to_csv("new_twits_only.csv", index=False, escapechar='\\')
print("Finished saving comments to CSV.")

if error_tickers:
    print("The following tickers caused errors or couldn't be scraped for the whole timeframe:")
    for ticker in error_tickers:
        print(ticker)
else:
    print("All tickers were scraped successfully.")

# -----------------------------------------------------------------------

os.system('terminal-notifier -message "Script has finished running!" -title "Notification" -sound "submarine"')