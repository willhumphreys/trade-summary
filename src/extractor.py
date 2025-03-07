# extractor.py
import os
import zipfile


def download_and_unzip_all_trades(symbol, output_dir, bucket_name, s3_client):
    """
    Downloads all the trade archives (ZIP files) for the specified symbol from the given S3 bucket.
    For each archive, it saves the ZIP file to an archive directory and unzips it into its own subdirectory under 'trades'.

    :param s3_client: The boto3 S3 client object.
    :param symbol: The symbol name (e.g. "btc-1mF")
    :param output_dir: The base output directory where archives and extracted files will be saved.
    :param bucket_name: The S3 bucket containing the archives.
    """

    # List all objects under the given symbol
    prefix = f"{symbol}/"
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        print(f"No files found under symbol: {symbol}")
        return

    # Create archive base directory for trades
    archives_dir = os.path.join(output_dir, "archives", "trades")
    os.makedirs(archives_dir, exist_ok=True)

    # Process each ZIP file
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".zip"):
            continue

        # Extract scenario name from the file name (e.g. "scenario1.zip" -> "scenario1")
        scenario = os.path.splitext(os.path.basename(key))[0]
        local_zip_file = os.path.join(archives_dir, f"{scenario}.zip")

        print(f"Downloading s3://{bucket_name}/{key} to {local_zip_file} ...")
        s3_client.download_file(bucket_name, key, local_zip_file)

        # Define output destination for extracted trade files into a scenario-specific folder
        destination_folder = os.path.join(output_dir, "trades", scenario)
        os.makedirs(destination_folder, exist_ok=True)

        print(f"Unzipping {local_zip_file} to {destination_folder} ...")
        with zipfile.ZipFile(local_zip_file, "r") as zf:
            zf.extractall(destination_folder)

        print(f"Done processing scenario: {scenario}")

    print("Done downloading and unzipping all scenarios.")
