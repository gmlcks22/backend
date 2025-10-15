
import json
import pandas as pd

# --- Configuration ---
QPOLL_DATA_PATH = 'qpoll_data.json'
MERGED_DATA_PATH = 'merged_data.json'
FINAL_OUTPUT_PATH = 'final_data.json'

# --- Main Execution ---
if __name__ == '__main__':
    try:
        # 1. Load the JSON files
        with open(QPOLL_DATA_PATH, 'r', encoding='utf-8') as f:
            qpoll_data = json.load(f)
        
        with open(MERGED_DATA_PATH, 'r', encoding='utf-8') as f:
            merged_data = json.load(f)

        print(f"Loaded {len(qpoll_data)} records from {QPOLL_DATA_PATH}")
        print(f"Loaded {len(merged_data)} records from {MERGED_DATA_PATH}")

        # 2. Create a lookup dictionary for the merged_data
        merged_data_lookup = {record['panel_id']: record for record in merged_data if 'panel_id' in record}

        # 3. Iterate through qpoll_data and merge
        final_data = []
        for qpoll_record in qpoll_data:
            panel_id = qpoll_record.get('panel_id')
            if panel_id in merged_data_lookup:
                # Get the corresponding record from merged_data
                demographic_record = merged_data_lookup[panel_id]
                
                # Create a new record starting with demographic data
                # and then add the survey data.
                # This avoids overwriting existing keys in qpoll_record if they conflict.
                combined_record = demographic_record.copy()
                combined_record.update(qpoll_record)
                final_data.append(combined_record)
            else:
                # If no matching demographic data, keep the qpoll record as is
                final_data.append(qpoll_record)

        # 4. Write the final merged data
        with open(FINAL_OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)

        print(f"\nSuccessfully merged the data.")
        print(f"Total records in final file: {len(final_data)}")
        print(f"Final data saved to '{FINAL_OUTPUT_PATH}'")

        if final_data:
            print("\n--- First Record Example ---")
            print(json.dumps(final_data[0], indent=4, ensure_ascii=True))

    except Exception as e:
        print(f"\n--- An error occurred during merging ---")
        print(f"Failed to merge files: {e}")
