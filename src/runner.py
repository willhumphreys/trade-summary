import argparse
import os
import shutil

import boto3

from extractor import download_and_unzip_all_trades
from src.filtered_summary_aggregator import aggregate_filtered_summary_files


def parse_arguments():
    parser = argparse.ArgumentParser(description="Download and unpack a trade archive from S3.")
    parser.add_argument("--symbol", required=True, help="The symbol name (e.g. 'btc-1mF')")
    return parser.parse_args()


def main():
    # Clean up the output directory
    output_dir = "output"
    if os.path.exists(output_dir):
        print(f"Deleting existing '{output_dir}' directory...")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Created fresh '{output_dir}' directory.")

    args = parse_arguments()
    output_directory = os.path.join(output_dir, args.symbol)

    os.makedirs(output_directory, exist_ok=True)

    s3_client = boto3.client("s3", region_name="eu-central-1")

    download_and_unzip_all_trades(args.symbol, output_directory, "mochi-prod-trade-performance-graphs", s3_client)
    formatted_trades_dir = os.path.join(output_directory, "trades", "formatted-trades")

    base_output_dir = os.path.join("output", args.symbol, "trades")

    # Define the path for the aggregated CSV file.
    aggregated_file_path = "output/aggregated_filtered_summary.csv"


    aggregate_filtered_summary_files(base_output_dir, aggregated_file_path)

    # Upload the aggregated CSV file to S3
    s3_bucket = "mochi-prod-final-trader-ranking"
    s3_key = f"{args.symbol}/aggregated_filtered_summary.csv"
    print(f"Uploading {aggregated_file_path} to s3://{s3_bucket}/{s3_key}...")

    s3_client.upload_file(
        Filename=aggregated_file_path,
        Bucket=s3_bucket,
        Key=s3_key
    )

    print(f"Upload complete. File available at s3://{s3_bucket}/{s3_key}")



if __name__ == "__main__":
    main()
