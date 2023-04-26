import re
import json
import requests
import pandas as pd
import os
import time
import random

BLOCKCHAIN_API_BASE = 'https://blockchain.info'

REPORTED_HACKER_ADDRESSES_FOLDER = '/home/covert/Desktop/blockchain/Blockchain-Trace/hacker_DB/DBdata'  # Update the path to your folder

def crawl_add():
    hackerList = []
    strad = "View address on blockchain.info"
    
    if not os.path.exists(REPORTED_HACKER_ADDRESSES_FOLDER):
        print(f"Directory not found: {REPORTED_HACKER_ADDRESSES_FOLDER}")
        return hackerList
    
    for file_name in os.listdir(REPORTED_HACKER_ADDRESSES_FOLDER):
        file_path = os.path.join(REPORTED_HACKER_ADDRESSES_FOLDER, file_name)
        
        if file_name.endswith('.csv'):
            forder = pd.read_csv(file_path)
            if forder.shape[0] > 0 and forder.shape[1] > 2:
                Adlist = forder.values[0][2]
                reallist = re.sub(strad, "", Adlist).strip()
                hackerList.append(reallist)
    return hackerList

def get_transactions(hacker_addresses, node):
    hacker_transactions = {}
    delay = 30
    max_delay = 60

    for address in hacker_addresses:
        response = requests.get(f'{node}/rawaddr/{address}')
        
        if response.status_code == 200:
            data = response.json()
            if 'txs' in data:
                hacker_transactions[address] = [tx['hash'] for tx in data['txs']]
        else:
            if response.status_code == 429:
                delay = min(delay * 2, max_delay)
                time.sleep(delay)

        time.sleep(delay + random.uniform(0, 3))

    return hacker_transactions

def main():
    hacker_addresses = crawl_add()
    node = BLOCKCHAIN_API_BASE
    hacker_transactions = get_transactions(hacker_addresses, node)
    
    output_filename = "Transaction_wallet_address.csv"
    data = []
    for address, tx_ids in hacker_transactions.items():
        for tx_id in tx_ids:
            data.append({'address': address, 'txid': tx_id})

    df = pd.DataFrame(data)
    df.to_csv(output_filename, index=False)

if __name__ == '__main__':
    main()
