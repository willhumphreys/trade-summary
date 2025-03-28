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

    return combined_df


def reorder_aggregated_summary(input_file, output_file):
    """
    Reorder the columns in the aggregated filtered summary to make Scenario first
    """
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)

        # Check if 'Scenario' column exists
        if 'Scenario' in df.columns:
            # Reorder columns to put 'Scenario' first
            cols = df.columns.tolist()
            cols.remove('Scenario')
            cols = ['Scenario'] + cols
            df = df[cols]

            # Save the reordered DataFrame
            df.to_csv(output_file, index=False)
            print(f"Reordered aggregated summary saved with 'Scenario' as first column to {output_file}")
            print(f"Columns order: {', '.join(df.columns[:5])}...")  # Print first 5 columns to verify order
            return df
        else:
            print(f"Warning: 'Scenario' column not found in {input_file}")
            # Just copy the file if no reordering is needed
            shutil.copy2(input_file, output_file)
            return pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reordering aggregated summary: {e}")
        # In case of error, just copy the original file
        shutil.copy2(input_file, output_file)
        return None


def sort_filtered_setups_by_summary(filtered_setups_df, summary_df, output_file):
    """
    Sort the filtered setups DataFrame to match the order of scenarios and trader IDs in the summary DataFrame
    """
    try:
        print("Sorting filtered setups to match the order in aggregated summary...")

        # Make sure we have the needed columns
        if 'scenario' not in filtered_setups_df.columns or 'traderid' not in filtered_setups_df.columns:
            print("Warning: Required columns not found in filtered setups. Skipping sorting.")
            return filtered_setups_df

        if 'Scenario' not in summary_df.columns or 'TraderID' not in summary_df.columns:
            print("Warning: Required columns not found in summary. Skipping sorting.")
            return filtered_setups_df

        # Create a mapping for sorting
        # First, create a DataFrame with just Scenario and TraderID from summary
        order_df = summary_df[['Scenario', 'TraderID']].copy()

        # Add a sort index to preserve the order
        order_df['sort_order'] = range(len(order_df))

        # Create a merged dataframe to get the sort order
        # Note: We're using left join to keep all the filtered_setups rows
        # even if they don't have a match in the summary
        merged_df = pd.merge(
            filtered_setups_df,
            order_df,
            how='left',
            left_on=['scenario', 'traderid'],
            right_on=['Scenario', 'TraderID']
        )

        # Sort the merged dataframe by the sort order
        # Use fillna with a large value to push non-matches to the end
        merged_df = merged_df.sort_values(
            by='sort_order',
            na_position='last'
        )

        # Drop the extra columns from the merge
        merged_df = merged_df.drop(columns=['Scenario', 'TraderID', 'sort_order'])

        # Save the sorted dataframe
        merged_df.to_csv(output_file, index=False)
        print(f"Sorted filtered setups saved to {output_file}")
        print(f"Total rows: {len(merged_df)}")

        return merged_df

    except Exception as e:
        print(f"Error sorting filtered setups: {e}")
        # In case of error, just save the original dataframe
        filtered_setups_df.to_csv(output_file, index=False)
        return filtered_setups_df


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

    # Define the path for temporary and final files
    temp_aggregated_file_path = "output/aggregated_filtered_summary.csv"
    final_aggregated_file_path = os.path.join(upload_dir, "aggregated_filtered_summary.csv")
    temp_filtered_setups_path = "output/temp_filtered_setups.csv"
    final_filtered_setups_path = os.path.join(upload_dir, "filtered-setups.csv")

    # Generate the aggregated filtered summary
    aggregate_filtered_summary_files(base_output_dir, temp_aggregated_file_path)

    # Reorder the aggregated summary to put Scenario first and save to upload directory
    summary_df = reorder_aggregated_summary(temp_aggregated_file_path, final_aggregated_file_path)

    # Copy graphs to the graphs directory inside upload using the reordered file
    copy_graphs_to_directory(args.symbol, final_aggregated_file_path, graphs_dir)

    # Aggregate all filtered setup files to a temporary file
    filtered_setups_df = aggregate_filtered_setup_files(temp_filtered_setups_path)

    # Sort the filtered setups to match the order in the summary and save to upload directory
    sort_filtered_setups_by_summary(filtered_setups_df, summary_df, final_filtered_setups_path)

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
