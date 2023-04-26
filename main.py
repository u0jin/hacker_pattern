import re
import json
import requests
import pandas as pd
import os
import time
import random
from datetime import datetime

BLOCKCHAIN_API_BASE = 'https://blockchain.info'

REPORTED_HACKER_ADDRESSES_FOLDER = '/Users/yujin/Desktop/Blockchain/hacker_DB/DBdata'
REPORTED_HACKER_TYPE_FOLDER = '/Users/yujin/Desktop/Blockchain/hacker_DB/Reportdata'

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

def get_transactions(hacker_address, node):
    hacker_transactions = []
    delay = 30
    max_delay = 60

    while True:
        response = requests.get(f'{node}/rawaddr/{hacker_address}')
        
        if response.status_code == 200:
            data = response.json()
            last_output_value = 0
            last_output_addr = None
            
            if 'txs' in data:
                for tx in data['txs']:
                    for output in tx['out']:
                        if 'addr' in output:
                            transaction_data = {
                                'sending_wallet': hacker_address,
                                'receiving_wallet': output['addr'],
                                'transaction_amount': output['value'] / 1e8,  # Convert to BTC
                                'coin_type': 'BTC',  # Assuming Bitcoin for this script
                                'date_sent': datetime.fromtimestamp(tx['time']).strftime('%Y-%m-%d'),
                                'time_sent': datetime.fromtimestamp(tx['time']).strftime('%H:%M:%S'),
                                'sending_wallet_source': 'Hacker DB',
                                'receiving_wallet_source': 'Blockchain.info'
                            }
                            hacker_transactions.append(transaction_data)
                            last_output_value = output['value']
                            last_output_addr = output['addr']
                            
            if last_output_value == 0:
                break
            else:
                hacker_address = last_output_addr
        else:
            if response.status_code == 429:
                delay = min(delay * 2, max_delay)
                time.sleep(delay)

        time.sleep(delay + random.uniform(0, 3))

    return hacker_transactions

def main():
    hackers_data = crawl_add()
    node = BLOCKCHAIN_API_BASE

    for hacker_data in hackers_data:
        hacker_address = hacker_data['hacker_address']
        report_type = hacker_data['report_type']
        hacker_transactions = get_transactions(hacker_address, node)

        output_filename = f"{report_type}.Transaction_{hacker_address}.csv"
        df = pd.DataFrame(hacker_transactions)
        df.to_csv(output_filename, index=False)


if __name__ == '__main__':
    main()
