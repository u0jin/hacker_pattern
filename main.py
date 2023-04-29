import re
import json
import requests
import pandas as pd
import os
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

BLOCKCHAIN_API_BASE = 'https://blockchain.info'

REPORTED_HACKER_ADDRESSES_FOLDER = '/Users/ujin/Desktop/Blockchain/hackerData/DBdata'
REPORTED_HACKER_TYPE_FOLDER = '/Users/ujin/Desktop/Blockchain/hackerData/Reportdata'
DATA_FILE_PATH = 'hacker_data.csv'

def load_hackers_data(file_path):
    hackers_data = []

    if not os.path.exists(file_path):
        print("File not found.")
        return hackers_data

    with open(file_path, 'r') as file:
        for line in file.readlines():
            hacker_address, report_type = line.strip().split(',')
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
def write_transaction_to_file(transaction_data, output_filename):
    if not os.path.exists(output_filename):
        with open(output_filename, 'w') as f:
            f.write(','.join(transaction_data.keys()) + '\n')

    with open(output_filename, 'a') as f:
        f.write(','.join(str(value) for value in transaction_data.values()) + '\n')

def get_transactions(hacker_address, report_type):
    hacker_transactions = []
    delay = 30
    max_delay = 60
    repeated_addresses_filename = 'repeated_addresses.txt'
    hacker_addresses_queue = [hacker_address]
    initial_hacker_addresses = {hacker_address}
    offset = 0
    node = BLOCKCHAIN_API_BASE
    output_filename = f"{report_type}.Transaction_{hacker_address}.csv"
    hacker_addresses_queue = [hacker_address]
    processed_addresses = set()


    while hacker_addresses_queue:
        current_hacker_address = hacker_addresses_queue.pop(0)
        response = requests.get(f'{node}/rawaddr/{current_hacker_address}?offset={offset}')

        if response.status_code == 200:
            print("Connected...")
            data = response.json()
            print("AD:",current_hacker_address)

            if 'txs' in data:
                for tx in data['txs']:
                    for output in tx['out']:
                        if 'addr' in output:
                            receiving_wallet = output['addr']
                            transaction_amount = output['value'] / 1e8
                            
                            transaction_data = {
                                    'tx_hash': tx['hash'],
                                    'sending_wallet': current_hacker_address,
                                    'receiving_wallet': receiving_wallet,
                                    'transaction_amount': transaction_amount,
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
                            write_transaction_to_file(transaction_data, output_filename)
                            if receiving_wallet not in processed_addresses:
                                hacker_addresses_queue.append(receiving_wallet)
                                processed_addresses.add(receiving_wallet)
                            else :
                                break  
                            print(transaction_amount)
                            if transaction_amount == 0:
                                break


                repeated_address = check_repeated_address(hacker_transactions)
                if repeated_address:
                    with open(repeated_addresses_filename, 'a') as f:
                        f.write(f"{repeated_address}\n")

                    hacker_addresses_queue.append(repeated_address)
                offset += 50
            else:
                break 

        else:
            if response.status_code == 429:
                delay = min(delay * 2, max_delay)
                time.sleep(delay)

        offset = 0 
        time.sleep(delay + random.uniform(0, 3))

    return hacker_transactions

def get_next_hacker_address(transactions):
    if transactions:
        last_transaction = transactions[-1]
        return last_transaction['receiving_wallet']
    return None

def process_hacker_data(hacker_data, node):
    hacker_address = hacker_data['hacker_address']
    report_type = hacker_data['report_type']
    output_filename = f"{report_type}.Transaction_{hacker_address}.csv"
    hacker_transactions = get_transactions(hacker_address, node, output_filename)

def main():
    DATA_FILE_PATH = '/Users/ujin/Desktop/Blockchain/hacker_data.csv'
    hackers_data = load_hackers_data(DATA_FILE_PATH)
    for hacker_data in hackers_data:
        get_transactions(hacker_data['hacker_address'], hacker_data['report_type'])


if __name__ == '__main__':
    main()