# extractor.py (or any other Python module as needed)
import os
import pandas as pd


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
                aggregated_df_list.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

    if aggregated_df_list:
        # Concatenate all the DataFrames
        combined_df = pd.concat(aggregated_df_list, ignore_index=True)

        # Sort by CompositeScore
        combined_df.sort_values(by="CompositeScore", inplace=True)

        # Write the aggregated data to a new CSV file
        combined_df.to_csv(aggregated_file_path, index=False)
        print(f"Aggregated CSV saved to: {aggregated_file_path}")
    else:
        print("No filtered_summary.csv files found.")


if __name__ == '__main__':
    # Define the directory where the trade summaries are located.
    # Adjust the base_output_dir if your output structure is different.
    base_output_dir = "output/btc-1mF/trades"

    # Define the path for the aggregated CSV file.
    aggregated_file_path = "output/aggregated_filtered_summary.csv"

    aggregate_filtered_summary_files(base_output_dir, aggregated_file_path)