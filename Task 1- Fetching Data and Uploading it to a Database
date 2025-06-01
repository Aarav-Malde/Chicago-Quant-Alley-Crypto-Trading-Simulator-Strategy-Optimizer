import requests
import datetime
import csv
import time

headers = {
    'Accept': 'application/json'
}

def get_btc_price():
    response = requests.get('https://api.india.delta.exchange/v2/tickers', headers=headers)
    if response.status_code == 200:
        data = response.json()
        tickers = data['result']
        for ticker in tickers:
            if ticker['symbol'] == 'BTCUSD':
                return float(ticker['mark_price'])
        raise Exception("BTCUSD ticker not found in response.")
    else:
        raise Exception(f"Failed to fetch BTC price: {response.text}")

weekday1 = '060525'
start_date = datetime.datetime.strptime(weekday1, "%d%m%y")

# List to collect data
rows = []

# Define correct CSV headers based on actual response
csv_headers = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

for options in ['P', 'C']:
    for i in range(7):  
        expiry_date = start_date + datetime.timedelta(days=i)
        expiry = expiry_date.strftime("%d%m%y")

        btc_price = get_btc_price()
        print(f"\nBTC futures price: {btc_price}")
        center = int(round(btc_price / 100) * 100)
        lower_strike = center - 10000
        upper_strike = center + 10000
        step = 100

        print(f"\n=== {options} options for expiry: {expiry} ===")
        for strike in range(lower_strike, upper_strike + 1, step):
            symbol = f'{options}-BTC-{strike}-{expiry}'

            # Dynamic start and end timestamps for each expiry
            start_dt = expiry_date - datetime.timedelta(days=7)
            start_ts = int(start_dt.replace(hour=0, minute=0, second=0).timestamp())
            end_ts = int(expiry_date.replace(hour=23, minute=59, second=59).timestamp())

            params = {
                'resolution': '5m',
                'symbol': symbol,
                'start': str(start_ts), 
                'end': str(end_ts)
            }

            response = requests.get('https://api.india.delta.exchange/v2/history/candles',
                                    params=params, headers=headers)
            print(f"Fetching {symbol} | Status: {response.status_code}")
            if response.status_code == 200:
                try:
                    candle_data = response.json().get('result', [])
                    for candle in candle_data:
                        rows.append([
                            symbol,
                            candle.get('time'),
                            candle.get('open'),
                            candle.get('high'),
                            candle.get('low'),
                            candle.get('close'),
                            candle.get('volume')
                        ])
                except Exception as e:
                    print(f"Error parsing data for {symbol}: {e}")
            else:
                print(f" - Error: {response.text}")

            time.sleep(0.5)

# Write all collected rows to CSV
with open('btc_options_data.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(csv_headers)
    writer.writerows(rows)

print(f"\n Data successfully saved to btc_options_data.csv ({len(rows)} rows)")
