from loguru import logger
import os
import yaml
from tqdm import tqdm
from minio import Minio
from minio.error import S3Error
import argparse

# Disable SSL warnings
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Minio client configuration
access_key = "m7sTvsz28Oq3AicEDHFo"
secret_key = "YVPGh367RnrT7G33lG6DtbaeuFZCqTE6KabMQClw"
endpoint_url = "airlab-share-01.andrew.cmu.edu:9000"
minio_client = Minio(
    endpoint_url,
    access_key=access_key,
    secret_key=secret_key,
    secure=True,
    cert_check=False,
)

# Bucket name
bucket_name = "tartandrive2"


def download_file(bucket, remote_path, local_path):
    """
    Downloads a file from the Minio bucket while displaying a progress bar
    that includes the download speed.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        # Get remote file size
        stat = minio_client.stat_object(bucket, remote_path)
        total_size = stat.size
    except S3Error as e:
        print(f"Error getting info for {remote_path}: {e}")
        return

    # Check if local file exists and determine its size
    local_size = 0
    if os.path.exists(local_path):
        local_size = os.path.getsize(local_path)
        if local_size >= total_size:
            print(f"{local_path} is already fully downloaded. Skipping.")
            return

    # Prepare headers to resume download
    request_headers = {}
    if local_size > 0:
        request_headers["Range"] = f"bytes={local_size}-"

    try:
        # Note: use request_headers parameter instead of headers.
        response = minio_client.get_object(
            bucket, remote_path, request_headers=request_headers
        )
    except S3Error as e:
        print(f"Error starting download for {remote_path}: {e}")
        return

    # Open file in append mode if resuming, otherwise write mode.
    mode = "ab" if local_size > 0 else "wb"
    chunk_size = 32 * 1024

    with open(local_path, mode) as file_data, tqdm(
        total=total_size,
        initial=local_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=os.path.basename(remote_path),
        dynamic_ncols=True,
    ) as pbar:
        for chunk in response.stream(chunk_size):
            file_data.write(chunk)
            pbar.update(len(chunk))

    response.close()
    response.release_conn()


def download_all_files(save_path, specific_bag_folder=None):
    """
    Downloads all files defined in the YAML mapping under the 'bags' key.
    If specific_bag_folder is provided (e.g. "bags/2023-10-26-14-42-35_turnpike_afternoon_fall"),
    only downloads files for that bag folder.
    """
    with open("assets/files.yaml", "r") as file:
        file_map = yaml.safe_load(file)
    bags_map = file_map.get("bags", {})

    if specific_bag_folder:
        # Ensure the folder key ends with a slash to match the YAML mapping keys
        key = (
            specific_bag_folder
            if specific_bag_folder.endswith("/")
            else specific_bag_folder + "/"
        )
        if key not in bags_map:
            print(f"Bag folder '{specific_bag_folder}' not found in YAML mapping.")
            return
        bags_map = {key: bags_map[key]}

    for bag_folder, content in bags_map.items():
        files = content.get("files", [])
        logger.info(f"Downloading {len(files)} files for folder '{bag_folder}'")
        for remote_file in files:
            # Construct local path preserving the structure
            local_file = os.path.join(save_path, remote_file)
            download_file(bucket_name, remote_file, local_file)
        logger.success(f"Downloaded {len(files)} files for folder '{bag_folder}'")
    logger.success("All bag folders downloaded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download all files defined for bag folders in assets/files.yaml from Minio."
    )
    parser.add_argument(
        "--save_path",
        type=str,
        required=True,
        help="Local directory where the files will be saved.",
    )
    parser.add_argument(
        "--bag_folder",
        type=str,
        default=None,
        help=(
            "Optional: Specific bag folder to download (e.g., 'bags/2023-10-26-14-42-35_turnpike_afternoon_fall'). "
            "If not provided, all bag folders in the YAML mapping will be downloaded."
        ),
    )
    args = parser.parse_args()

    download_all_files(args.save_path, args.bag_folder)
