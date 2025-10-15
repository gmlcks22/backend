import pandas as pd
import json
import numpy as np
import glob

# --- Configuration ---
INPUT_FILES = glob.glob('data/Quickpoll/qpoll*.xlsx')
OUTPUT_JSON_PATH = 'qpoll_data.json'

# Mapping for fixed demographic columns to English
COLUMN_MAPPING = {
    '구분': 'category',
    '고유번호': 'panel_id',
    '성별': 'gender',
    '나이': 'age_raw',
    '지역': 'region',
    '설문일시': 'survey_timestamp'
}

def process_qpoll_file(path):
    """
    Processes a single qpoll-formatted Excel file into a structured DataFrame.
    """
    try:
        xlsx = pd.ExcelFile(path)

        # 1. Read Sheet 2 to build the value label map
        df_labels = xlsx.parse(xlsx.sheet_names[1], header=None)
        ids = df_labels.iloc[0, 1:].values
        labels = df_labels.iloc[1, 1:].values
        value_label_map = {str(id_).strip(): label for id_, label in zip(ids, labels) if pd.notna(id_)}

        # 2. Read survey title from Sheet 1, cell A1
        survey_title = xlsx.parse(xlsx.sheet_names[0], header=None).iloc[0, 0]

        # 3. Read Sheet 1 for the data, using the second row as the header
        df_data = xlsx.parse(xlsx.sheet_names[0], header=1)

        # 4. Identify the multi-select column (column G, index 6)
        multi_select_col_name = df_data.columns[6]

        # 5. Define a function to apply labels from numeric IDs
        def apply_labels_from_numbers(value):
            if pd.isna(value):
                return []
            id_string = str(value)
            labeled_answers = []
            ids = id_string.split(',')
            for id_val in ids:
                id_val = id_val.strip()
                if not id_val:
                    continue
                try:
                    num_id = int(float(id_val))
                    key = f"보기{num_id}"
                    labeled_answers.append(value_label_map.get(key, f"Unknown ID: {key}"))
                except (ValueError, TypeError):
                    continue
            return labeled_answers

        # 6. Apply the function to the multi-select column
        df_data[multi_select_col_name] = df_data[multi_select_col_name].apply(apply_labels_from_numbers)

        # 7. Add the survey question text as a new column
        df_data['survey_question'] = survey_title

        # 8. Rename the multi-select answers column to a generic key
        df_data.rename(columns={multi_select_col_name: 'survey_answers'}, inplace=True)

        # 9. Rename all demographic columns to English
        df_data.rename(columns=COLUMN_MAPPING, inplace=True)

        # 10. Clean up NaN values
        return df_data.replace({np.nan: None})

    except Exception as e:
        print(f"Error processing file {path}: {e}")
        raise

# --- Main Execution ---
if __name__ == '__main__':
    all_data_by_panel = {}
    try:
        for file_path in INPUT_FILES:
            print(f"Processing {file_path}...")
            processed_df = process_qpoll_file(file_path)
            
            for col in processed_df.select_dtypes(include=['datetime64[ns]']).columns:
                processed_df[col] = processed_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

            for record in processed_df.to_dict('records'):
                panel_id = record.get('panel_id')
                if not panel_id:
                    continue

                if panel_id not in all_data_by_panel:
                    all_data_by_panel[panel_id] = {
                        'panel_id': panel_id,
                        'category': record.get('category'),
                        'gender': record.get('gender'),
                        'age_raw': record.get('age_raw'),
                        'region': record.get('region'),
                        'surveys': []
                    }
                
                survey_data = {
                    'survey_question': record.get('survey_question'),
                    'survey_answers': record.get('survey_answers'),
                    'survey_timestamp': record.get('survey_timestamp')
                }
                all_data_by_panel[panel_id]['surveys'].append(survey_data)

        final_records = list(all_data_by_panel.values())
        
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_records, f, ensure_ascii=False, indent=4)

        print(f"\nSuccessfully processed {len(INPUT_FILES)} file(s).")
        print(f"Total unique users processed: {len(final_records)}")
        print(f"Data saved to '{OUTPUT_JSON_PATH}'")

        if final_records:
            print("\n--- First Record Example ---")
            print(json.dumps(final_records[0], indent=4, ensure_ascii=False))

    except Exception as e:
        print(f"\n--- An error occurred during execution ---")
        print(f"Failed to process files: {e}")