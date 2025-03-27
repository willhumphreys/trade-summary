# extractor.py (or any other Python module as needed)
import os
import pandas as pd
import re


def aggregate_filtered_summary_files(base_output_dir, aggregated_file_path):
    """
    Aggregates all filtered_summary.csv files under base_output_dir, sorts them by CompositeScore,
    and writes the aggregated data to aggregated_file_path.

    :param base_output_dir: The base output directory where filtered_summary.csv files reside.
    :param aggregated_file_path: The output file path to write the aggregated CSV.
    """
    aggregated_df_list = []

    # Recursively walk the directory tree and find all filtered_summary.csv files
    for root, dirs, files in os.walk(base_output_dir):
        if "filtered_summary.csv" in files:
            file_path = os.path.join(root, "filtered_summary.csv")
            print(f"Processing file: {file_path}")

            try:
                df = pd.read_csv(file_path)

                # Extract the parameter pattern
                param_pattern = re.search(r'(s_[^/]+?)(?=/summary|$)', file_path)
                scenario = param_pattern.group(1) if param_pattern else "unknown_scenario"

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
        print("No filtered_summary.csv files found or all files were empty.")


if __name__ == '__main__':
    # Define the directory where the trade summaries are located.
    # Adjust the base_output_dir if your output structure is different.
    base_output_dir = "output/btc-1mF/trades"

    # Define the path for the aggregated CSV file.
    aggregated_file_path = "output/aggregated_filtered_summary.csv"

    aggregate_filtered_summary_files(base_output_dir, aggregated_file_path)
