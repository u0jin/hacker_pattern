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
    hacker_addresses = []
    report_types = []

    if not os.path.exists(REPORTED_HACKER_ADDRESSES_FOLDER) or not os.path.exists(REPORTED_HACKER_TYPE_FOLDER):
        print("Required directories not found.")
        return hacker_addresses, report_types

    for file_name in os.listdir(REPORTED_HACKER_ADDRESSES_FOLDER):
        address_file_path = os.path.join(REPORTED_HACKER_ADDRESSES_FOLDER, file_name)
        type_file_name = file_name.replace("DB", "report")
        type_file_path = os.path.join(REPORTED_HACKER_TYPE_FOLDER, type_file_name)

        if file_name.endswith('.csv') and os.path.exists(type_file_path):
            address_forder = pd.read_csv(address_file_path)
            type_forder = pd.read_csv(type_file_path)

            if not address_forder.empty and not type_forder.empty:
                hacker_address = address_forder.iloc[0, 2].strip()
                hacker_addresses.append(hacker_address)
                
                report_type = type_forder.iloc[0, 2]
                report_types.append(report_type)

    return hacker_addresses, report_types

def get_transactions(hacker_addresses):
    transactions = []

    for address in hacker_addresses:
        response = requests.get(f'{BLOCKCHAIN_API_BASE}/rawaddr/{address}')

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
                                'Hacker_Address': address,
                                'Sending_Wallet': sending_wallet,
                                'Receiving_Wallet': receiving_wallet,
                                'Transaction_Amount': amount,
                                'Coin_Type': 'BTC',  # Assuming Bitcoin for now
                                'Date_Sent': tx_time.split(' ')[0],
                                'Time_Sent': tx_time.split(' ')[1],
                                'Sending_Wallet_Source': 'Unknown',  # Assuming unknown sources for now
                                'Receiving_Wallet_Source': 'Unknown',  # Assuming unknown sources for now
                            }
                            transactions.append(transaction)
        else:
            print(f"Error fetching transactions for address {address}: {response.status_code}")

        time.sleep(delay + random.uniform(0, 3))  # Add some randomness to the delay between requests.

    return transactions

def main():
    hacker_addresses, report_types = crawl_add()
    transactions = get_transactions(hacker_addresses)

    for i, transaction in enumerate(transactions):
        transaction['Report_Type'] = report_types[hacker_addresses.index(transaction['Hacker_Address'])]

    df = pd.DataFrame(transactions)

       # Group the DataFrame by Report_Type
    grouped_df = df.groupby('Report_Type')

    # Save each group to a separate CSV file
    for report_type, group in grouped_df:
        output_file = f"{report_type}_Transaction_wallet_name.csv"
        group.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

if __name__ == '__main__':
    main()

