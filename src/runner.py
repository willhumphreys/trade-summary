import argparse
import os
import shutil
import csv
import glob
import pandas as pd

import boto3

from extractor import download_and_unzip_all_trades
from src.filtered_summary_aggregator import aggregate_filtered_summary_files


def parse_arguments():
    parser = argparse.ArgumentParser(description="Download and unpack a trade archive from S3.")
    parser.add_argument("--symbol", required=True, help="The symbol name (e.g. 'btc-1mF')")
    return parser.parse_args()


def copy_graphs_to_directory(symbol, aggregated_file_path, output_graph_dir):
    """
    Copy graphs from their original locations to a consolidated graphs directory
    based on entries in the aggregated_filtered_summary.csv
    """
    # Create graphs directory if it doesn't exist
    os.makedirs(output_graph_dir, exist_ok=True)
    print(f"Created '{output_graph_dir}' directory for consolidated graphs.")

    # Read the aggregated summary file
    with open(aggregated_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            trader_id = row['TraderID']
            scenario = row['Scenario']

            # Construct the source path pattern to find the graph
            source_pattern = os.path.join(
                "output",
                f"{symbol}*",
                "trades",
                scenario,
                "graphs",
                f"trades-and-profit-{trader_id}.png"
            )

            # Find all files matching the pattern
            matching_files = glob.glob(source_pattern)

            if matching_files:
                for source_file in matching_files:
                    # Create destination filename
                    destination_file = os.path.join(
                        output_graph_dir,
                        f"{symbol}_{scenario}_{trader_id}.png"
                    )

                    # Copy the file
                    shutil.copy2(source_file, destination_file)
                    print(f"Copied: {source_file} -> {destination_file}")
            else:
                print(f"Warning: No graph found for trader {trader_id} in scenario {scenario}")

    print(f"Graph copying complete. All graphs copied to {output_graph_dir}")


def aggregate_filtered_setup_files(output_file):
    """
    Find all filtered setup CSV files and combine them into a single file.
    Files follow the pattern: output/*/trades/*/trades/filtered-*.csv
    """
    # Find all filtered setup files
    file_pattern = os.path.join("output", "*", "trades", "*", "trades", "filtered-*.csv")
    setup_files = glob.glob(file_pattern)

    if not setup_files:
        print("No filtered setup files found.")
        return

    print(f"Found {len(setup_files)} filtered setup files to aggregate.")

    # Read and combine all files
    dfs = []
    for file_path in setup_files:
        try:
            # Extract symbol and scenario info from the path
            path_parts = file_path.split(os.path.sep)
            symbol = path_parts[1].split("_")[0]  # Extract symbol from directory name

            # Extract scenario from the filename part after "filtered-"
            filename = os.path.basename(file_path)
            if filename.startswith("filtered-"):
                scenario = filename[len("filtered-"):].rsplit('.', 1)[
                    0]  # Remove "filtered-" prefix and ".csv" extension
            else:
                # Fallback to extracting from the directory path
                scenario = path_parts[3]

            print(f"Extracted scenario: {scenario}")

            # Read the CSV file
            df = pd.read_csv(file_path)

            # Add scenario column as first column, and symbol column
            df.insert(0, 'scenario', scenario)  # Insert scenario as the first column
            df['Symbol'] = symbol

            dfs.append(df)
            print(f"Added data from {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    if not dfs:
        print("No valid data found in any files.")
        return

    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    # If the DataFrame has an unnamed index column, drop it
    if any(col.startswith('Unnamed: 0') for col in combined_df.columns):
        drop_cols = [col for col in combined_df.columns if col.startswith('Unnamed: 0')]
        combined_df = combined_df.drop(columns=drop_cols)

    # Reorder columns if 'rank' exists to ensure scenario is first, then rank
    if 'rank' in combined_df.columns:
        cols = combined_df.columns.tolist()
        cols.remove('rank')
        cols.remove('scenario')
        cols = ['scenario', 'rank'] + cols
        combined_df = combined_df[cols]

    # Save the combined data
    combined_df.to_csv(output_file, index=False)
    print(f"Aggregated filtered setup data saved to {output_file}")
    print(f"Total rows: {len(combined_df)}")
    print(f"Columns order: {', '.join(combined_df.columns[:5])}...")  # Print first 5 columns to verify order


def main():
    # Clean up the output directory
    output_dir = "output"
    if os.path.exists(output_dir):
        print(f"Deleting existing '{output_dir}' directory...")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Created fresh '{output_dir}' directory.")

    # Create upload directory
    upload_dir = os.path.join(output_dir, "upload")
    os.makedirs(upload_dir, exist_ok=True)
    print(f"Created '{upload_dir}' directory for final outputs.")

    # Create graphs directory inside upload directory
    graphs_dir = os.path.join(upload_dir, "graphs")
    os.makedirs(graphs_dir, exist_ok=True)

    args = parse_arguments()
    output_directory = os.path.join(output_dir, args.symbol)

    os.makedirs(output_directory, exist_ok=True)

    s3_client = boto3.client("s3", region_name="eu-central-1")

    download_and_unzip_all_trades(args.symbol, output_directory, "mochi-prod-trade-performance-graphs", s3_client)
    formatted_trades_dir = os.path.join(output_directory, "trades", "formatted-trades")

    base_output_dir = os.path.join("output", args.symbol, "trades")

    # Define the path for the temporary and final aggregated CSV files
    temp_aggregated_file_path = "output/aggregated_filtered_summary.csv"
    final_aggregated_file_path = os.path.join(upload_dir, "aggregated_filtered_summary.csv")

    # Generate the aggregated filtered summary
    aggregate_filtered_summary_files(base_output_dir, temp_aggregated_file_path)

    # Copy graphs to the graphs directory inside upload
    copy_graphs_to_directory(args.symbol, temp_aggregated_file_path, graphs_dir)

    # Aggregate all filtered setup files to upload directory
    filtered_setups_path = os.path.join(upload_dir, "filtered-setups.csv")
    aggregate_filtered_setup_files(filtered_setups_path)

    # Copy the aggregated_filtered_summary.csv to upload directory
    shutil.copy2(temp_aggregated_file_path, final_aggregated_file_path)
    print(f"Copied: {temp_aggregated_file_path} -> {final_aggregated_file_path}")

    # Upload the aggregated CSV file to S3
    s3_bucket = "mochi-prod-final-trader-ranking"
    s3_key = f"{args.symbol}/aggregated_filtered_summary.csv"
    print(f"Uploading {final_aggregated_file_path} to s3://{s3_bucket}/{s3_key}...")

    s3_client.upload_file(
        Filename=final_aggregated_file_path,
        Bucket=s3_bucket,
        Key=s3_key
    )

    print(f"Upload complete. File available at s3://{s3_bucket}/{s3_key}")
    print(f"All aggregated files and graphs are available in the '{upload_dir}' directory.")


if __name__ == "__main__":
    main()
