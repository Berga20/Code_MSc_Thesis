import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import warnings
import os
import json

#--------------------------------------------------------------

def scrape_stock(tkr, stop_date, max_twits):
        """
        Args:
            tkr (string): Ticker of the stock whose tweets want to be scraped
            stop_date (string): DATE MUST BE OF FORMAT "YYYY-MM-DD". Insert the date up to which scrape starting from the current day. 

        Returns:
            pd.DataFrame: _description_
        """
        warnings.simplefilter(action='ignore', category=FutureWarning)
        options = webdriver.ChromeOptions()
        driver = webdriver.Remote(command_executor='http://localhost:4444', options=options)
        login_url = f"https://stocktwits.com/signin?next=/"
        url = f"https://stocktwits.com/symbol/{tkr}"
        driver.get(login_url)
        time.sleep(2)
        
        # Logging in
        email = 'wildersteun@gmail.com'
        password = 'WTFeyenoord99!'
        driver.find_element(By.NAME, "login").send_keys(email)
        time.sleep(3)
        driver.find_element(By.NAME, "password").send_keys(password)
        time.sleep(3)
        driver.find_element(By.XPATH, '//*[@id="Layout"]/div[1]/div[3]/div/div[2]/form/button').click()
        time.sleep(3)

        # Starting the scraping
        
        driver.get(url)
        time.sleep(3)

        # Accepting Coockies button
        driver.find_element(By.XPATH, '//*[@id="onetrust-accept-btn-handler"]').click()

        time.sleep(2)

        # Closing Invasive Ads Button
        try:
            driver.find_element(By.XPATH, '//*[@id="Layout"]/div[4]/div/div[1]/button').click()
        except:
            pass

        time.sleep(3)

        df = pd.DataFrame(
                {
                'Ticker':[], 
                'User':[], 
                'Date':[], 
                'Message': [], 
        })

        scroll_count = 0
        last_date = datetime.today().strftime('%Y-%m-%d')
        start_time = datetime.now()
        n_unique_twits = 0

        while stop_date < last_date and n_unique_twits <= max_twits:
            
            soup = BeautifulSoup(driver.page_source, 'lxml')
            posts = soup.find_all('div', class_ = 'StreamMessage_main__qWCNf')

            for item in posts:
                try:
                    user = item.find('span', {'aria-label': 'Username'}).text
                    message = item.find('div', class_ = 'RichTextMessage_body__4qUeP').text
                    post_date = datetime.strptime(item.find('time', {'aria-label': 'Time this message was posted'}).get('datetime'), '%Y-%m-%dT%H:%M:%SZ').date().strftime('%Y-%m-%d')
                    df = df.append({
                        'Ticker': tkr, 
                        'User': user, 
                        'Date': post_date,
                        'Message': message
                    }, ignore_index=True)
                    
                    n_unique_twits = len(df['Message'].unique())
                    last_date = df['Date'].tail(1).values[0]
                    
                except:
                    pass 

            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(3)
            scroll_count += 1
            
        if n_unique_twits >= max_twits:
            print(f'{tkr}: maximum number of twits exceeded [{scroll_count} Scrolls]')
        else:
            print(f'{tkr} scraping complete: {scroll_count} Scrolls')
            
        driver.quit()
        df = df.drop_duplicates(subset='Message')

        return df
    
#-------------------------

sp500_data = pd.read_csv('sp500_04-05-2024.csv', header=0, index_col=0)
sp500_data


def update_progress(ticker, oldest_date):
    companies_scraped[ticker] = oldest_date
    with open(progress_file, 'w') as f:
        json.dump({'companies_scraped': companies_scraped}, f)

    
progress_file = 'scraping_progress.json'

if os.path.exists(progress_file):
    with open(progress_file, 'r') as f:
        progress = json.load(f)
        companies_scraped = progress.get('companies_scraped', {})
else:
    companies_scraped = {}


def update_progress(ticker, oldest_date):
    companies_scraped[ticker] = oldest_date
    with open(progress_file, 'w') as f:
        json.dump({'companies_scraped': companies_scraped}, f)

def scrape_and_update(ticker):

    stop_date = '2024-01-01'
    max_twits = 500

    df = scrape_stock(tkr=ticker, stop_date=stop_date, max_twits=max_twits)
    
    if not df.empty:
        oldest_date = df['Date'].min()
        update_progress(ticker, oldest_date)
        print(f'{ticker} scraped until {oldest_date}')
    else:
        print(f'No data for {ticker}')

    return df


ticker_list = sp500_data['ticker']
tickers_to_scrape = [tkr for tkr in ticker_list if tkr not in companies_scraped][0:4] #4 companies per execution
    
    
with ThreadPoolExecutor(max_workers=4) as executor:
    results = executor.map(scrape_and_update, tickers_to_scrape)
    
new_combined_df = pd.concat(results, ignore_index=True)

if os.path.exists('stocktwits_data.csv'):
    existing_df = pd.read_csv('stocktwits_data.csv')
    combined_df = pd.concat([existing_df, new_combined_df], ignore_index=True)
else:
    combined_df = new_combined_df

combined_df = combined_df.drop_duplicates(subset='Message')

n_companies_scraped = len(new_combined_df['Ticker'].unique())
print(f'------------------[SCRAPING COMPLETED]------------------ \n {n_companies_scraped} companies scraped')
combined_df.to_csv('stocktwits_data.csv', index=False)