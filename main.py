import os
import pandas as pd
import re

REPORTED_HACKER_TYPE_FOLDER = "/Users/ujin/Desktop/Blockchain/hackerDB/Reportdata"
REPORTED_HACKER_ADDRESSES_FOLDER = '/Users/ujin/Desktop/Blockchain/hackerDB/DBdata'

def crawl_add():
    hacker_addresses = []
    report_types = []
    strad = "View address from blockchain.info"
    
    # Check if the directories exist
    if not os.path.exists(REPORTED_HACKER_ADDRESSES_FOLDER):
        print(f"Directory not found: {REPORTED_HACKER_ADDRESSES_FOLDER}")
        return hacker_addresses, report_types

    if not os.path.exists(REPORTED_HACKER_TYPE_FOLDER):
        print(f"Directory not found: {REPORTED_HACKER_TYPE_FOLDER}")
        return hacker_addresses, report_types
    
    for file_name in os.listdir(REPORTED_HACKER_ADDRESSES_FOLDER):
        address_file_path = os.path.join(REPORTED_HACKER_ADDRESSES_FOLDER, file_name)
        type_file_name = file_name.replace("DB", "report")
        type_file_path = os.path.join(REPORTED_HACKER_TYPE_FOLDER, type_file_name)

        if file_name.endswith('.csv') and os.path.exists(type_file_path):
            address_forder = pd.read_csv(address_file_path)
            type_forder = pd.read_csv(type_file_path)

            if address_forder.shape[0] > 0 and address_forder.shape[1] > 2 and type_forder.shape[0] > 0 and type_forder.shape[1] > 2: 
                Adlist = address_forder.values[0][2]
                reallist = re.sub(strad, "", Adlist).strip()
                hacker_addresses.append(reallist)
                
                report_type = type_forder.values[0][2]
                report_types.append(report_type)
    
    return hacker_addresses, report_types

def main():
    hacker_addresses, report_types = crawl_add()

    # Create a DataFrame with the hacker addresses and report types
    data = {'Hacker_Address': hacker_addresses, 'Report_Type': report_types}
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    output_file = "hacker_addresses_report_types.csv"
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

if __name__ == '__main__':
    main()
