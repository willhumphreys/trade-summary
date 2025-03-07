import argparse
import os
import shutil

import boto3

from extractor import download_and_unzip_trades



def parse_arguments():
    parser = argparse.ArgumentParser(description="Download and unpack a trade archive from S3.")
    parser.add_argument("--symbol", required=True, help="The symbol name (e.g. 'btc-1mF')")
    parser.add_argument("--scenario", required=True,
                        help="The scenario string (e.g. 's_-3000..-100..400___l_100..7500..400___...')")
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
    output_directory = os.path.join(output_dir, args.symbol, args.scenario)

    os.makedirs(output_directory, exist_ok=True)

    s3_client = boto3.client("s3")

    download_and_unzip_trades(args.symbol, args.scenario, output_directory, "mochi-trade-analysis", s3_client)
    formatted_trades_dir = os.path.join(output_directory, "trades", "formatted-trades")


if __name__ == "__main__":
    main()
