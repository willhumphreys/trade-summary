# extractor.py (or any other Python module as needed)
import os
import pandas as pd
import re


def aggregate_filtered_summary_files(base_output_dir, aggregated_file_path):
    """
    Aggregates all files ending with _filtered_summary.csv under base_output_dir,
    sorts them by CompositeScore, and writes the aggregated data to aggregated_file_path.

    :param base_output_dir: The base output directory where _filtered_summary.csv files reside.
    :param aggregated_file_path: The output file path to write the aggregated CSV.
    """
    aggregated_df_list = []

    # Recursively walk the directory tree and find all files ending with _filtered_summary.csv
    for root, dirs, files in os.walk(base_output_dir):
        for filename in files:
            if filename.endswith("_filtered_summary.csv"):
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")

                try:
                    df = pd.read_csv(file_path)

                    # Extract the scenario from the directory path
                    # The back_test_id will always be in downloaded keys
                    # The directory structure is: output/symbol/trades/backTestId___scenario_params
                    parts = root.split(os.path.sep)
                    if len(parts) >= 4 and 'trades' in parts:
                        # Find the index of 'trades' in the path
                        trades_index = parts.index('trades')
                        # The scenario should be the part after 'trades'
                        if trades_index + 1 < len(parts):
                            scenario = parts[trades_index + 1]
                        else:
                            scenario = "unknown_scenario"
                    else:
                        scenario = "unknown_scenario"


                    # Add scenario column to the DataFrame - handle both possible column names
                    if 'TraderID' in df.columns:
                        traderId_idx = df.columns.get_loc('TraderID')
                        df.insert(traderId_idx + 1, 'Scenario', scenario)
                    elif 'traderId' in df.columns:
                        traderId_idx = df.columns.get_loc('traderId')
                        df.insert(traderId_idx + 1, 'Scenario', scenario)
                    else:
                        # If neither column exists, add the scenario as the first column
                        df.insert(0, 'Scenario', scenario)

                    aggregated_df_list.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    if aggregated_df_list and len(aggregated_df_list) > 0:
        # Concatenate all the DataFrames
        combined_df = pd.concat(aggregated_df_list, ignore_index=True)

        # Sort by CompositeScore if it exists
        if 'CompositeScore' in combined_df.columns:
            combined_df.sort_values(by="CompositeScore", inplace=True, ascending=False)

        # Write the aggregated data to a new CSV file
        combined_df.to_csv(aggregated_file_path, index=False)
        print(f"Aggregated CSV saved to: {aggregated_file_path}")
    else:
        print("No files ending with _filtered_summary.csv found or all files were empty.")
        # Consider if throwing an exception here is still the desired behavior
        raise Exception("No valid files to aggregate.")


if __name__ == '__main__':
    # Define the directory where the trade summaries are located.
    # Adjust the base_output_dir if your output structure is different.
    base_output_dir = "output/btc-1mF/trades"

    # Define the path for the aggregated CSV file.
    aggregated_file_path = "output/aggregated_filtered_summary.csv"

    aggregate_filtered_summary_files(base_output_dir, aggregated_file_path)
