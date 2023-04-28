import re
import json
import requests
import pandas as pd
import os
import time
import random
from datetime import datetime

BLOCKCHAIN_API_BASE = 'https://blockchain.info'

REPORTED_HACKER_ADDRESSES_FOLDER = '/home/covert/Desktop/blockchain/hakerData/DBdata'
REPORTED_HACKER_TYPE_FOLDER = '/home/covert/Desktop/blockchain/hakerData/Reportdata'
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

def check_repeated_address(transactions, threshold=1):
    address_counts = {}
    for transaction in transactions:
        receiving_wallet = transaction['receiving_wallet']
        if receiving_wallet in address_counts:
            address_counts[receiving_wallet] += 1
        else:
            address_counts[receiving_wallet] = 1

    for address, count in address_counts.items():
        if count > threshold:
            return address

    return None

def get_next_hacker_address(transactions):
    if transactions:
        last_transaction = transactions[-1]
        return last_transaction['receiving_wallet']
    return None

def get_transactions(hacker_address, node):
    hacker_transactions = []
    delay = 30
    max_delay = 60
    repeated_addresses_filename = 'repeated_addresses.txt'
    hacker_addresses_queue = [hacker_address]

    while hacker_addresses_queue:
        current_hacker_address = hacker_addresses_queue.pop(0)

        response = requests.get(f'{node}/rawaddr/{current_hacker_address}')
        print(current_hacker_address)

        if response.status_code == 200:
            print("Connected...")
            data = response.json()
            balance = data.get('final_balance', 0)
            print(balance)

            if 'txs' in data:
                for tx in data['txs']:
                    for output in tx['out']:
                        if 'addr' in output:
                            receiving_wallet = output['addr']
                            transaction_data = {
                                'tx_hash': tx['hash'],
                                'sending_wallet': current_hacker_address,
                                'receiving_wallet': receiving_wallet,
                                'transaction_amount': output['value'] / 1e8,
                                'coin_type': 'BTC',
                                'date_sent': datetime.fromtimestamp(tx['time']).strftime('%Y-%m-%d'),
                                'time_sent': datetime.fromtimestamp(tx['time']).strftime('%H:%M:%S'),
                                'sending_wallet_source': 'Hacker DB',
                                'receiving_wallet_source': 'Blockchain.info',
                                'input_addresses': [inp['prev_out']['addr'] for inp in tx['inputs'] if 'prev_out' in inp and 'addr' in inp['prev_out']],
                                'output_addresses': [out['addr'] for out in tx['out'] if 'addr' in out],
                                'total_input_value': sum([inp['prev_out']['value'] for inp in tx['inputs'] if 'prev_out' in inp and 'value' in inp['prev_out']]) / 1e8,
                                'total_output_value': sum([out['value'] for out in tx['out'] if 'value' in out]) / 1e8,
                                'fee': tx['fee'] / 1e8
                            }
                            hacker_transactions.append(transaction_data)
                            hacker_addresses_queue.append(transaction_data['receiving_wallet'])

                repeated_address = check_repeated_address(hacker_transactions)
                if repeated_address:
                    with open(repeated_addresses_filename, 'a') as f:
                        f.write(f"{repeated_address}\n")

                    hacker_addresses_queue.append(repeated_address)

            if balance == 0 :
                break
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