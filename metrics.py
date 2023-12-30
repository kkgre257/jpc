import pandas as pd
import os 
import numpy as np
import time 
import json
import re 

def convert_event_payload(event_payload):

    event_payload = event_payload.strip('{}')
    pairs = event_payload.split(', ')
    parsed_payload = {}

    for pair in pairs:
        if '=' in pair:
            key, value = pair.split('=')
            key = key.strip()

            if value.startswith('{') and value.endswith('}'):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass
            else:
                value = value.strip('"')

            parsed_payload[key] = value

    # convert dict to JSON object
    json_payload = json.dumps(parsed_payload, indent=4)
    # print(parsed_payload)
    return(json_payload)

def load_or_cache_data(csv_file_path):
    try:
        df = pd.read_pickle('cached_data.pkl')

        # Print data type before conversion
        print("Data type before conversion:")
        print(df['event_payload'].apply(type))  # Print the data types of each element

        # convert comma separated 'event_payload' to JSON format
        df['event_payload'] = df['event_payload'].apply(lambda x: convert_event_payload(x) if isinstance(x, str) else x)

        # Print data type after conversion
        print("\nData type after conversion:")
        print(df['event_payload'].apply(type))  # Print the data types of each element

    except FileNotFoundError:
        df = pd.read_csv(csv_file_path)

        # Print data type before conversion
        print("Data type before conversion:")
        print(df['event_payload'].apply(type))  # Print the data types of each element

        # convert comma separated 'event_payload' to JSON format
        df['event_payload'] = df['event_payload'].apply(lambda x: convert_event_payload(x) if isinstance(x, str) else x)

        # Print data type after conversion
        print("\nData type after conversion:")
        print(df['event_payload'].apply(type))  # Print the data types of each element

        df.to_pickle('cached_data.pkl')
    return df

targets_config = {
    'target1': {
        # update w/ path to CSV file
        'csv_file_path': '/Users/kgreen634/Downloads/mar4thdiagnostic200.csv', 
    },
    # 'target2':{
    #     # updatae w/ path to CSV file
    #     'csv_file_path':'/path/to/downloaded_file2.csv',
    # s
}

for target, config in targets_config.items():
    csv_file_path = config['csv_file_path']
    

    if os.path.exists(csv_file_path):
        df = load_or_cache_data(csv_file_path)

        # perform and return data analysis for each target here
        
        # DUPE STATS
    #     duplicate_counts = df[df.duplicated(subset=['event_id', 'event_name'], keep = False)].copy()
    #     duplicate_counts['dupe_count'] = duplicate_counts.groupby(['event_id', 'event_name'])['event_id'].transform('count') - 1 
    #     unique_duplicate_counts = duplicate_counts.drop_duplicates(subset=['event_id', 'event_name', 'dupe_count'])
        
    #     total_dupes = unique_duplicate_counts['dupe_count'].sum()

    #     total_records_count = len(df)

    #     unique_duplicate_events = duplicate_counts['event_id'].nunique()

    #     max_duplicates_per_event = duplicate_counts['dupe_count'].max()

    #     min_duplicates_per_event = duplicate_counts['dupe_count'].min()

    #     percentage_dupes = round((unique_duplicate_events / (total_records_count - total_dupes)) * 100, 2 )
        
    # #     # dupe time difference
    #     duplicate_counts['approximate_arrival_timestamp'] = pd.to_datetime(duplicate_counts['approximate_arrival_timestamp'])
    #     duplicate_counts['time_diff'] = duplicate_counts.groupby(['event_id', 'event_name'])['approximate_arrival_timestamp'].transform(lambda x: x - x.min())
    #     average_time_diff_group = duplicate_counts.groupby(['event_id', 'event_name'])['time_diff'].mean().round('1min')
    #     max_time_diff_all = duplicate_counts['time_diff'].max().round('1min')
    #     min_time_diff_all = duplicate_counts['time_diff'].min().round('1min')
    #     average_time_diff_all = duplicate_counts['time_diff'].mean().round('1min')

    #     print(f"Max Time Difference for All Duplicates: {max_time_diff_all}")
    #     print(f"Min Time Difference for All Duplicates: {min_time_diff_all}")
    #     print(f"Average Time Difference for All Duplicates: {average_time_diff_all}")
    #     print(f"Total Dupes: {total_dupes}")
    #     print(f"Total Records: {total_records_count}")
    #     print(f"Unique Duplicate Events: {unique_duplicate_events}")
    #     print(f"Percentage of Dupes: {percentage_dupes}%")
    #     print(f"Max Duplicates per Event: {max_duplicates_per_event}")
    #     print(f"Min Duplicates per Event: {min_duplicates_per_event}")

    #     # MIN/MAX/AVG AMT OF TIME BTW EVENT CREATION & SUCCESSFUL RECEIPT BY API
    #     df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
    #     df['approximate_arrival_timestamp'] = pd.to_datetime(df['approximate_arrival_timestamp'], errors='coerce')
    #     df['time_diff_hours'] = df.apply(lambda row: pd.NaT if pd.isna(row['approximate_arrival_timestamp']) or pd.isna(row['timestamp']) else (row['approximate_arrival_timestamp'] - row['timestamp']).total_seconds() / 3600, axis=1)

    #     min_time_diff_hours = df['time_diff_hours'].min()
    #     max_time_diff_hours = df['time_diff_hours'].max()
    #     avg_time_diff_hours = df['time_diff_hours'].mean()

    #     print(f"Min Time Diff (hours): {min_time_diff_hours}")
    #     print(f"Max Time Diff (hours): {max_time_diff_hours}")
    #     print(f"Avg Time Diff (hours): {avg_time_diff_hours}")


    # #     # RETRY SUCCESS & IMPACT STATS
    # #     # NOT WORKING event_payload_json is different data than whats in excel file of same data
        total_rows_in_search_df = df.shape[0]
        successful_retried_count = 0
        unsuccessful_retry_count = 0
        total_number_retried_events = 0 
        total_retry_attempts = 0
        sum_of_given_retry_dict_total = 0 

        # df['event_payload'] = df['event_payload'].astype(str)
        # print("Data type of 'event_payload' after conversion:", df['event_payload'].dtype)


        for index, row in df.iterrows():
            event_payload_json = row['event_payload']
            print("event payload json:")
            print(event_payload_json)
            
            if pd.isna(event_payload_json) or 'retried=' not in event_payload_json:
                # print("we continued")
                continue

            event_payload_json = event_payload_json
            
            retried_match = re.search(r'retried=({.*?})', event_payload_json)

            

            if retried_match:
                print("WE HAVE A MATCH!!!!!!!!!!!!!!!!!!")
                retried_dict_str = retried_match.group(1)
                retried_dict = eval(retried_dict_str)

            if isinstance(event_payload_json, dict) and 'retried' in event_payload_json:
                retried_dict = event_payload_json.get('retried')

            
                if isinstance(retried_dict, dict) and 'total' in retried_dict:
                    for key,value in retried_dict.items():
                        if key != 'total':
                            total_number_retried_events += 1 
                            total_retry_attempts += value 
                            if key in df['event_id'].values:
                                successful_retried_count += 1 
                            else:
                                unsuccessful_retry_count += 1 
                        elif key == 'total':
                            sum_of_given_retry_dict_total += value 
            
    #     print(total_retry_attempts)
    #     print(total_number_retried_events)
    #     avg_retries_per_event = round(total_retry_attempts / total_number_retried_events, 2)
    #     retry_success_percent = round((successful_retried_count / total_number_retried_events) * 100, 2) 
    #     percent_of_data_resulting_from_confirmed_successful_retry = round((successful_retried_count / total_rows_in_search_df) * 100, 2)


    #     print("Total count of confirmed successful retries:", successful_retried_count)
    #     print("Total count of confirmed unsuccessful retries:", unsuccessful_retry_count)
    #     print("Total number of retried data events:", total_number_retried_events)
    #     print("Retry success percentage:", retry_success_percent,"%")
    #     print("Total number of retry attempts from listed event ids:", total_retry_attempts)
    #     print("The total number of retries if all totals from the retry dicts were added:", sum_of_given_retry_dict_total)
    #     print("Average # of retries per retried event:", avg_retries_per_event)
    #     print("Percent of data resulting from confirmed successful retries:", percent_of_data_resulting_from_confirmed_successful_retry,"%")

        
    else:   
        print(f"CSV file for {target} not found at {csv_file_path}. Skipping.")


