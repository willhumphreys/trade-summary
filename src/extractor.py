# extractor.py
import os
import boto3
import zipfile


def download_and_unzip_trades(symbol, scenario, output_dir, bucket_name, s3_client):
    """
    Downloads the specified trade archive from the given S3 bucket,
    saves the ZIP file to an archive directory, and then unzips it into an output directory under 'trades'.

    :param s3_client: The boto3 S3 client object.
    :param symbol: The symbol name (e.g. "btc-1mF")
    :param scenario: The scenario name (e.g. "s_-3000..-100..400___...")
    :param output_dir: The base output directory where archives and extracted files will be saved.
    :param bucket_name: The S3 bucket containing the archives.
    """

    object_key = f"{symbol}/{scenario}.zip"

    # Create archive destination for trades
    archives_dir = os.path.join(output_dir,"archives", "trades")
    os.makedirs(archives_dir, exist_ok=True)
    local_zip_file = os.path.join(archives_dir, f"{scenario}.zip")

    print(f"Downloading s3://{bucket_name}/{object_key} to {local_zip_file} ...")
    s3_client.download_file(bucket_name, object_key, local_zip_file)

    # Define output destination for extracted trade files (directly under 'trades')
    destination_folder = os.path.join(output_dir, "trades")
    os.makedirs(destination_folder, exist_ok=True)

    print(f"Unzipping {local_zip_file} to {destination_folder} ...")
    with zipfile.ZipFile(local_zip_file, "r") as zf:
        zf.extractall(destination_folder)

    print("Done downloading and unzipping.")
