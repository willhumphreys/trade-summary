import argparse
import os
import shutil
import csv
import glob
import pandas as pd

import boto3

from extractor import download_and_unzip_all_trades
from filtered_summary_aggregator import aggregate_filtered_summary_files


def parse_arguments():
    parser = argparse.ArgumentParser(description="Process trade summary data, aggregate, rank, and upload.")
    parser.add_argument("--symbol", required=True, help="Symbol to process (e.g., btc-1mF)")
    # --- Add the new flag ---
    parser.add_argument(
        "--skip-download",
        action="store_true", # Sets args.skip_download to True if flag is present
        help="If set, skip deleting the output directory and downloading/unzipping files."
    )
    # Add other arguments if they exist
    return parser.parse_args()



def copy_graphs_to_directory(symbol, aggregated_file_path, output_graph_dir, output_trades_dir):
    """
    Copy graphs from their original locations to a consolidated graphs directory
    based on entries in the aggregated_filtered_summary.csv
    :param output_trades_dir: 
    """
    # Create graphs directory if it doesn't exist
    os.makedirs(output_graph_dir, exist_ok=True)
    print(f"Created '{output_graph_dir}' directory for consolidated graphs.")

    os.makedirs(output_trades_dir, exist_ok=True)
    print(f"Created '{output_trades_dir}' directory for consolidated trades.")

    # Read the aggregated summary file
    with open(aggregated_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            trader_id = row['TraderID']
            scenario = row['Scenario']

            # Construct the source path pattern to find the graph
            graph_source_pattern = os.path.join(
                "output",
                f"{symbol}*",
                "trades",
                scenario,
                "graphs",
                f"trades-and-profit-{trader_id}.png"
            )

            # Find all files matching the pattern
            matching_files = glob.glob(graph_source_pattern)

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

            # Construct the source path pattern to find the graph
            trade_source_pattern = os.path.join(
                "output",
                f"{symbol}*",
                "trades",
                scenario,
                "trades",
                "formatted-trades",
                f"{trader_id}.csv"
            )

            # Find all files matching the pattern
            matching_trades_files = glob.glob(trade_source_pattern)

            if matching_trades_files:
                for trade_source_file in matching_trades_files:
                    # Create destination filename
                    destination_file = os.path.join(
                        output_trades_dir,
                        f"{symbol}_{scenario}_{trader_id}.csv"
                    )
                    # Copy the file
                    shutil.copy2(trade_source_file, destination_file)
                    print(f"Copied: {trade_source_file} -> {destination_file}")
            else:
                print(f"Warning: No trades found for trader {trader_id} in scenario {scenario}")

    print(f"Trade and graph copying complete. All trades copied to {output_graph_dir} and {output_trades_dir}")


def aggregate_filtered_setup_files(output_file):
    """
    Find all filtered setup CSV files and combine them into a single file.
    Files follow the pattern: output/*/trades/*/trades/filtered-*.csv
    The back_test_id will always be in downloaded keys.
    """
    # Find all filtered setup files
    file_pattern = os.path.join("output", "*", "trades", "*", "trades", "filtered-*.csv")
    setup_files = glob.glob(file_pattern)

    if not setup_files:
        print("No filtered setup files found.")
        raise Exception("No filtered setup files found.")

    print(f"Found {len(setup_files)} filtered setup files to aggregate.")

    # Read and combine all files
    dfs = []
    for file_path in setup_files:
        try:
            # Extract symbol and scenario info from the path
            path_parts = file_path.split(os.path.sep)
            symbol = path_parts[1].split("_")[0]  # Extract symbol from directory name

            # Extract scenario from the directory path
            # The directory structure is: output/symbol/trades/backTestId___scenario_params/trades/filtered-*.csv
            # The scenario is the part after 'trades' and before the next 'trades'
            if 'trades' in path_parts:
                # Find the first occurrence of 'trades' in the path
                trades_index = path_parts.index('trades')
                # The scenario should be the part after the first 'trades'
                if trades_index + 1 < len(path_parts):
                    scenario = path_parts[trades_index + 1]
                else:
                    scenario = "unknown_scenario"
            else:
                scenario = "unknown_scenario"

            print(f"Extracted scenario: {scenario}")

            # Read the CSV file
            df = pd.read_csv(file_path)

            # Add scenario column as first column, and symbol column
            df.insert(0, 'scenario', scenario)  # Insert scenario as the first column
            df['Symbol'] = symbol

            dfs.append(df)
            print(f"Added data from {file_path}")
        except Exception as e:
            # Check if the error message indicates an empty or unparsable CSV file
            if "No columns to parse from file" in str(e):
                print(f"Warning: Skipping empty or unparsable file {file_path}. Reason: {e}")
                continue  # Skip to the next file in the loop
            else:
                # For any other type of exception, print the error and re-raise it
                print(f"Error processing {file_path}: {e}")
                raise Exception(f"Error processing {file_path}: {e}")

    if not dfs:
        print("No valid data found in any files.")
        raise Exception("No valid data found in any files.")

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



def sort_filtered_setups_by_summary(filtered_setups_df, summary_df, output_file):
    """
    Sort the filtered setups DataFrame to match the order of scenarios and trader IDs in the summary DataFrame
    """

    print("Sorting filtered setups to match the order in aggregated summary...")

    # Make sure we have the needed columns
    if 'scenario' not in filtered_setups_df.columns or 'traderid' not in filtered_setups_df.columns:
        print("Warning: Required columns ('scenario', 'traderid') not found in filtered setups. Skipping sorting.")
        # Save the original dataframe if columns are missing
        filtered_setups_df.to_csv(output_file, index=False)
        return filtered_setups_df

    if 'Scenario' not in summary_df.columns or 'TraderID' not in summary_df.columns:
        print("Warning: Required columns ('Scenario', 'TraderID') not found in summary. Skipping sorting.")
        # Save the original dataframe if columns are missing
        filtered_setups_df.to_csv(output_file, index=False)
        return filtered_setups_df

    # Create a mapping for sorting
    # First, create a DataFrame with just Scenario and TraderID from summary
    order_df = summary_df[['Scenario', 'TraderID']].copy()

    # Rename summary columns to lowercase to match filtered_setups_df for the merge
    order_df.rename(columns={'Scenario': 'scenario', 'TraderID': 'traderid'}, inplace=True)

    # Add a sort index to preserve the order from the summary
    order_df['sort_order'] = range(len(order_df))

    # Create a merged dataframe to get the sort order
    # Use left join to keep all the filtered_setups rows
    # Now we can use 'on' because the column names match
    merged_df = pd.merge(
        filtered_setups_df,
        order_df,
        how='left',
        on=['scenario', 'traderid'] # Use 'on' now that names match
    )

    # Check for rows that didn't merge (indicating potential data mismatches)
    unmerged_count = merged_df['sort_order'].isna().sum()
    if unmerged_count > 0:
        print(f"Warning: {unmerged_count} rows in filtered setups did not find a match in the summary.")
        raise Exception(f"Warning: {unmerged_count} rows in filtered setups did not find a match in the summary.")

    # Sort the merged dataframe by the sort order
    # Use fillna with a large value to push non-matches (NaNs) to the end
    merged_df = merged_df.sort_values(
        by='sort_order',
        na_position='last' # Keep non-matched rows at the end
    )

    # Drop the extra column used for sorting
    merged_df = merged_df.drop(columns=['sort_order'])

    # Save the sorted dataframe
    merged_df.to_csv(output_file, index=False)
    print(f"Sorted filtered setups saved to {output_file}")
    print(f"Total rows: {len(merged_df)}")
    if unmerged_count > 0:
        print(f"Note: {unmerged_count} rows without a summary match are placed at the end.")
        raise Exception(f"Warning: {unmerged_count} rows without a summary match are placed at the end.")


    return merged_df


def create_setups_file(filtered_setups_df, output_file):
    """
    Create a simplified setups.csv file with only specific columns and formatting
    """

    print("Creating simplified setups.csv file...")

    # Check if filtered_setups_df has all the required columns
    required_columns = ['traderid', 'dayofweek', 'hourofday', 'stop', 'limit', 'tickoffset', 'tradeduration',
                        'outoftime']

    # Check for any missing columns
    missing_columns = [col for col in required_columns if col not in filtered_setups_df.columns]

    if missing_columns:
        # Special case for 'hourofday' column which might have a comment
        if 'hourofday' in missing_columns:
            possible_hourofday_columns = [col for col in filtered_setups_df.columns if col.startswith('hourofday')]
            if possible_hourofday_columns:
                # Use the first matching column as 'hourofday'
                filtered_setups_df = filtered_setups_df.rename(columns={possible_hourofday_columns[0]: 'hourofday'})
                missing_columns.remove('hourofday')

        if missing_columns:
            print(f"Warning: Missing required columns in filtered setups: {missing_columns}")
            return None

    # Extract only the required columns
    setups_df = filtered_setups_df[required_columns].copy()

    # Add a sequential row number as the first column
    setups_df.insert(0, '', range(1, len(setups_df) + 1))

    # Format the 'hourofday' column to include the file comment if not already present
    if ' #file:runner.py ' not in str(setups_df['hourofday'].iloc[0]):
        setups_df = setups_df.rename(columns={'hourofday': 'hourofday #file:runner.py '})

    # Save the dataframe with the specified format
    # Use quoting=csv.QUOTE_ALL to quote all fields
    setups_df.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)

    print(f"Simplified setups file created at {output_file}")
    print(f"Total rows: {len(setups_df)}")

    return setups_df



def add_rank_column_to_filtered_setups(df):
    """
    Add a rank column as the first column to the filtered setups DataFrame
    """
    if df is not None:
        print("Adding rank column to filtered setups...")
        df.insert(0, 'Rank', range(1, len(df) + 1))
        return df
    return None


def add_rank_column_to_summary(df):
    """
    Add a rank column as the first column to the summary DataFrame
    """
    if df is not None:
        print("Adding rank column to summary...")
        # # Check if Scenario is already the first column
        # if df.columns[0] == 'Scenario':
        #     # Insert after Scenario
        #     df.insert(1, 'Rank', range(1, len(df) + 1))
        # else:
            # Insert as first column
        df.insert(0, 'Rank', range(1, len(df) + 1))
        return df
    return None


def main():
    args = parse_arguments() # Parse arguments first


    # No longer using back_test_id as we're summarizing all back_test_ids
    print("Processing all back test IDs for the symbol")

    output_dir = "output"

    # --- Conditional Directory Cleanup ---
    if not args.skip_download:
        if os.path.exists(output_dir):
            print(f"Deleting existing '{output_dir}' directory...")
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created fresh '{output_dir}' directory.")
    else:
        print(f"Skipping output directory deletion as --skip-download is set.")
        # Ensure the base output directory exists even if skipping deletion
        os.makedirs(output_dir, exist_ok=True)

    # Create upload directory (always needed, even if skipping download)
    upload_dir = os.path.join(output_dir, "upload")
    os.makedirs(upload_dir, exist_ok=True)
    print(f"Ensured '{upload_dir}' directory exists.")

    # Create graphs directory inside upload directory
    graphs_dir = os.path.join(upload_dir, "graphs")
    os.makedirs(graphs_dir, exist_ok=True)

    trades_dir = os.path.join(upload_dir, "trades")
    os.makedirs(trades_dir, exist_ok=True)

    # Define the symbol-specific output directory path
    output_directory = os.path.join(output_dir, args.symbol)
    os.makedirs(output_directory, exist_ok=True) # Ensure it exists

    s3_client = boto3.client("s3", region_name="eu-central-1")

    # --- Conditional Download ---
    if not args.skip_download:
        print("Proceeding with download and extraction...")
        # Make sure download_and_unzip_all_trades is defined or imported correctly
        # Passing None for back_test_id to process all back test IDs
        download_and_unzip_all_trades(args.symbol, output_directory, "mochi-prod-trade-performance-graphs", s3_client)
    else:
        print(f"Skipping download and extraction for {args.symbol} as --skip-download is set.")

    # --- The rest of the main function remains largely the same ---
    # Define paths relative to the potentially existing structure
    formatted_trades_dir = os.path.join(output_directory, "trades", "formatted-trades")
    # The base_output_dir should be the parent directory of all the scenario directories
    # Since the files are extracted to output/[symbol]/trades/[scenario]
    base_output_dir = os.path.join("output", args.symbol) # This path depends on download structure

    # Define the path for temporary and final files
    # Using os.path.join for better cross-platform compatibility
    temp_aggregated_file_path = os.path.join(output_dir, "aggregated_filtered_summary.csv")
    temp_aggregated_with_rank_path = os.path.join(output_dir, "aggregated_filtered_summary_with_rank.csv")
    final_aggregated_file_path = os.path.join(upload_dir, "aggregated_filtered_summary.csv")
    temp_filtered_setups_path = os.path.join(output_dir, "temp_filtered_setups.csv")
    temp_sorted_filtered_setups_path = os.path.join(output_dir, "sorted_filtered_setups.csv")
    final_filtered_setups_path = os.path.join(upload_dir, "filtered-setups.csv")
    final_setups_path = os.path.join(upload_dir, "setups.csv")

    # --- (The rest of your processing steps: aggregation, ranking, copying, uploading) ---
    # Ensure these steps correctly handle the case where data already exists if --skip-download is used.
    # For example, aggregate_filtered_summary_files should read from the existing base_output_dir.

    # Generate the aggregated filtered summary
    # This should work correctly as it reads from base_output_dir which exists whether downloaded or skipped
    aggregate_filtered_summary_files(base_output_dir, temp_aggregated_file_path)

    # Reorder the aggregated summary to put Scenario first
    summary_df = reorder_aggregated_summary(temp_aggregated_file_path, temp_aggregated_with_rank_path)

    # Add rank column to summary
    summary_with_rank_df = add_rank_column_to_summary(summary_df)

    # Save the summary with rank to the final path
    if summary_with_rank_df is not None:
        summary_with_rank_df.to_csv(final_aggregated_file_path, index=False)
        print(f"Saved summary with rank to {final_aggregated_file_path}")

    # Copy graphs to the graphs directory inside upload using the final file
    # This also relies on the base_output_dir structure existing
    copy_graphs_to_directory(args.symbol, final_aggregated_file_path, graphs_dir, trades_dir)

    # Aggregate all filtered setup files to a temporary file
    # This function needs to correctly find files within the potentially pre-existing base_output_dir structure
    filtered_setups_df = aggregate_filtered_setup_files(temp_filtered_setups_path)

    # Sort the filtered setups to match the order in the summary
    sorted_filtered_setups_df = sort_filtered_setups_by_summary(filtered_setups_df, summary_with_rank_df,
                                                                temp_sorted_filtered_setups_path)

    # Add rank column to filtered setups
    filtered_setups_with_rank_df = add_rank_column_to_filtered_setups(sorted_filtered_setups_df)

    # Save the filtered setups with rank to the final path
    if filtered_setups_with_rank_df is not None:
        filtered_setups_with_rank_df.to_csv(final_filtered_setups_path, index=False)
        print(f"Saved filtered setups with rank to {final_filtered_setups_path}")

    # Create the simplified setups.csv file
    create_setups_file(filtered_setups_with_rank_df, final_setups_path)

    # Upload all files from the upload directory to S3
    s3_bucket = "mochi-prod-final-trader-ranking"
    # No longer using back_test_id as we're summarizing all back_test_ids
    base_s3_key = args.symbol

    print(f"Uploading contents of '{upload_dir}' to s3://{s3_bucket}/{base_s3_key}/...")

    # Walk through the upload directory
    for root, dirs, files in os.walk(upload_dir):
        for filename in files:
            # Get the local file path
            local_path = os.path.join(root, filename)

            # Calculate the relative path from the upload directory
            relative_path = os.path.relpath(local_path, upload_dir)

            # Create the S3 key by joining the base key with the relative path
            # Handle potential Windows path separators
            s3_key = f"{base_s3_key}/{relative_path.replace(os.path.sep, '/')}"


            print(f"Uploading {local_path} to s3://{s3_bucket}/{s3_key}...")

            # Upload the file
            s3_client.upload_file(
                Filename=local_path,
                Bucket=s3_bucket,
                Key=s3_key
            )

    print(f"Upload complete. All files from '{upload_dir}' uploaded to s3://{s3_bucket}/{base_s3_key}/")


if __name__ == "__main__":
    main()
