import pandas as pd
import logging
import os
import zipfile
import json
import sys
# from kslib import remove_file


logging.basicConfig(format='%(asctime)s - %(levelname)s:%(name)s:%(message)s', level=logging.INFO)
logger = logging.getLogger('Kompsh.lde')
logger.setLevel(level=os.getenv('LOG_LEVEL', 'DEBUG').upper())

DEVICES_DATA_PATH = 'KOMPIO_795_daily_dump_from_Listener_and_Listener_error_tables/2023-11-23.csv'
DEVICES_ERROR_DATA_PATH = 'KOMPIO_795_daily_dump_from_Listener_and_Listener_error_tables/2023-11-23.err.csv'

def remove_file(csv_path):
    if os.path.exists(csv_path):
        os.remove(csv_path)

def run_script():
    if not DEVICES_DATA_PATH and not DEVICES_ERROR_DATA_PATH:
        logger.error("No device data or error data were given")
        sys.exit(-1)
    
    list_of_csv_files = []
    
    device_data_df = group_device_wise_data_csv(DEVICES_DATA_PATH)
    device_data_error_df = group_device_wise_error_csv(DEVICES_ERROR_DATA_PATH)
        
    if device_data_df is None and device_data_error_df is None:
        logger.error("No valid data found in either file")
        sys.exit(-1)

    if not device_data_df.empty and not device_data_error_df.empty: 
        merged_data = pd.concat([device_data_df, device_data_error_df], axis=0, ignore_index=True)
        logger.debug(" %s merged data from listener", merged_data)
    else:
        merged_data = device_data_df if not device_data_df.empty else device_data_error_df
    
    merged_data['last_modified'] = pd.to_datetime(merged_data['last_modified'])
    merged_data.sort_values(by='last_modified', ascending=False, inplace=True)
    logger.debug("sorted DataFrame based on timestamp %s", merged_data)
    merged_data['device'] = merged_data['device'].apply(lambda x: x.upper() if x is not None else x)
    merged_data.groupby('device')
    unique_devices = merged_data['device'].unique()
    logger.debug("Unique devices in the DataFrame %s", unique_devices)
    for device in unique_devices:
        unique_device_data = merged_data[merged_data['device'] == device]
        if not pd.isna(device) and not device.isnumeric():
            output_path = f"{device}_device_data.csv"
            unique_device_data.to_csv(output_path, index=False)
            logger.debug('%s device csv is created', device)
            list_of_csv_files.append(output_path)   
    combined_zip_file_name_for_both_device_data_and_device_error = DEVICES_DATA_PATH.replace(".csv", '_device_wise_data.zip')
    combined_zip_generated(combined_zip_file_name_for_both_device_data_and_device_error, list_of_csv_files)
    

 
# Device wise data   
def group_device_wise_data_csv(csv_file_path):
    device_data_df = pd.read_csv(csv_file_path)
    logger.debug("The device data DataFrame %s", device_data_df)
    return device_data_df if len(device_data_df) !=0 else None

# Device wise error data
def group_device_wise_error_csv(csv_file_path_for_error):
    device_data_error_df = pd.read_csv(csv_file_path_for_error)
    device_data_error_df['device'] = None
    for index, row in device_data_error_df.iterrows():
        try:
            data = json.loads(row['data'])
            if 'device' in data and not data['device'].isnumeric():
                device_data_error_df.at[index, 'device'] = data['device']
            elif 'accesstoken' in data and not data['accesstoken'].isnumeric():
                device_data_error_df.at[index, 'device'] = data['accesstoken']
            else:
                logger.debug("No device named device or accesstoken")
        except json.JSONDecodeError:
            logger.debug("Invalid Json format %s", row)
            data = row['data']
            if 'accesstoken' in data:
                device_name = find_device_from_invalid_json(data, 'accesstoken')
                device_data_error_df.at[index, 'device'] = device_name
            elif 'device' in data:
                device_name = find_device_from_invalid_json(data, 'device')
                device_data_error_df.at[index, 'device'] = device_name
            else:
                logger.debug('No device named device or accesstoken')
    return device_data_error_df if len(device_data_error_df) !=0 else None

# find device from invalid json
def find_device_from_invalid_json(invalid_data, device_key):
    logger.debug('Getting device name from invalid error data %s', invalid_data)
    comma_separated_data = invalid_data.split(",")
    device_key_name = None
    for key_str in comma_separated_data:
        if device_key in key_str:
            logger.debug("Found device key and name pair string %s", key_str)
            device_key_name = key_str
            break
    if device_key_name:
        device_name = device_key_name.split(":")[-1].replace('"', '')
        logger.debug("Device name found in invalid data %s", device_name)
        return device_name
    logger.debug("Unable to find device name from invalid data")


# Generate Combined zip file 
def combined_zip_generated(combined_zip_file_name_for_both_device_data_and_device_error, list_of_csv_files):
    logger.debug('combining all csv files into one zip file')
    with zipfile.ZipFile(combined_zip_file_name_for_both_device_data_and_device_error, 'w') as combine_zip_generate:
        for input_device in list_of_csv_files:
            combine_zip_generate.write(input_device, arcname=input_device)
            remove_file(input_device)
            logger.debug(f"{input_device} file is removed ")

run_script()