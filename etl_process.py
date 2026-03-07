import pandas as pd
import glob
import os
import logging

logging.basicConfig(
    filename='etl_log_etl1.log',       
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

print("Starting pipeline successfully")  
logging.info("--ETL JOB started")

try:
    all_data_frames = []
    files = glob.glob("input data/*")
    print(f"Found {len(files)} files to process")  

    for file in files:
        if file.endswith(".csv"):
            print(f"Reading csv: {file}")
            df = pd.read_csv(file)
            all_data_frames.append(df)
        elif file.endswith(".json"):
            print(f"Reading json: {file}")
            df = pd.read_json(file)
            all_data_frames.append(df)

    if len(all_data_frames) > 0:
        combined_df = pd.concat(all_data_frames)
        print("Combined data frames (sources)")
    else:
        raise ValueError("There are no sources in the list")

    if combined_df['price'].isnull().sum() > 0:
        combined_df.dropna(subset=['price'], inplace=True)

    combined_df['quantity'] = combined_df['quantity'].fillna(0)
    combined_df['total_value'] = combined_df['price'] * combined_df['quantity'] 
    combined_df.drop_duplicates(inplace=True)

    print("Completed transformation")
    logging.info("Data transformation completed")

    if not os.path.exists("output data"):
        os.makedirs("output data")

    output_path = 'output data/transformed_file.csv'
    combined_df.to_csv(output_path, index=False)  
    print("Completed loading")
    logging.info("--ETL JOB completed successfully")  

except Exception as e:
    print(f"ETL failed: {e}")            # fix: generic message
    logging.error(f"ETL failed: {e}")   # fix: logging.error
