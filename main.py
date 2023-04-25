import os
import pandas as pd
import re
import json
import requests
import time
import random
from datetime import datetime

# Constants
REPORTED_HACKER_TYPE_FOLDER = "/Users/ujin/Desktop/Blockchain/hackerDB/Reportdata"
REPORTED_HACKER_ADDRESSES_FOLDER = '/Users/ujin/Desktop/Blockchain/hackerDB/DBdata'
BLOCKCHAIN_API_BASE = 'https://blockchain.info'
delay = 10
max_delay = 60

def crawl_add():
    hackers_data = []

    if not os.path.exists(REPORTED_HACKER_ADDRESSES_FOLDER) or not os.path.exists(REPORTED_HACKER_TYPE_FOLDER):
        print("Required directories not found.")
        return hackers_data

    for file_name in os.listdir(REPORTED_HACKER_ADDRESSES_FOLDER):
        address_file_path = os.path.join(REPORTED_HACKER_ADDRESSES_FOLDER, file_name)
        type_file_name = file_name.replace("DB", "report")
        type_file_path = os.path.join(REPORTED_HACKER_TYPE_FOLDER, type_file_name)

        if file_name.endswith('.csv') and os.path.exists(type_file_path):
            address_forder = pd.read_csv(address_file_path)
            type_forder = pd.read_csv(type_file_path)

            if not address_forder.empty and not type_forder.empty:
                hacker_address = re.sub(r'\s*View address on blockchain.info.*$', '', address_forder.iloc[0, 2].strip())
                report_type = type_forder.iloc[0, 2]
                
                hackers_data.append({'hacker_address': hacker_address, 'report_type': report_type})

    return hackers_data


def get_transactions(hacker_address):
    transactions = []

    response = requests.get(f'{BLOCKCHAIN_API_BASE}/rawaddr/{hacker_address}')

    if response.status_code == 200:
        data = response.json()

        if 'txs' in data:
            for tx in data['txs']:
                tx_time = datetime.fromtimestamp(tx['time']).strftime('%Y-%m-%d %H:%M:%S')

                for input in tx['inputs']:
                    sending_wallet = input['prev_out']['addr']
                    amount = input['prev_out']['value'] / 100000000

                    for output in tx['out']:
                        receiving_wallet = output['addr']
                        transaction = {
                            'Hacker_Address': hacker_address,
                            'Sending_Wallet': sending_wallet,
                            'Receiving_Wallet': receiving_wallet,
                            'Transaction_Amount': amount,
                            'Coin_Type': 'BTC',
                            'Date_Sent': tx_time.split(' ')[0],
                            'Time_Sent': tx_time.split(' ')[1],
                            'Sending_Wallet_Source': 'Unknown',
                            'Receiving_Wallet_Source': 'Unknown',
                        }
                        transactions.append(transaction)

    elif response.status_code == 404:
        print(f"Skipping transactions for address {hacker_address}: {response.status_code} - Address not found or no transactions associated with it.")
    else:
        print(f"Error fetching transactions for address {hacker_address}: {response.status_code}")

    time.sleep(delay + random.uniform(0, 3))

    return transactions

def main():
    hackers_data = crawl_add()
    for index, hacker_data in enumerate(hackers_data):
        transactions = get_transactions([hacker_data['hacker_address']])
        print(transactions)

        for i, transaction in enumerate(transactions):
            transaction['Report_Type'] = hacker_data['report_type']

        df = pd.DataFrame(transactions)

        output_file = f"{hacker_data['report_type'].replace(' ', '_')}_Transaction_wallet_name_{index + 1}.csv"
        columns = ['Sending_Wallet', 'Receiving_Wallet', 'Transaction_Amount', 'Coin_Type', 'Coin_Type', 'Date_Sent', 'Time_Sent', 'Sending_Wallet_Source', 'Receiving_Wallet_Source']

        if not df.empty:
            df[columns].to_csv(output_file, index=False, header=False)
            print(f"Data saved to {output_file}")
        else:
            print("No transactions found.")
            with open(output_file, 'w') as file:
                file.write(f"Hacker_Address: {hacker_data['hacker_address']}\n")
                file.write(f"Report_Type: {hacker_data['report_type']}\n")
            print(f"Data saved to {output_file}")

if __name__ == '__main__':
    main()
