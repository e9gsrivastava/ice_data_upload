"""
script to upload ice data to aws s3 bucket 
"""
import os
import argparse
import gzip
from concurrent.futures import ThreadPoolExecutor
from zipfile import ZipFile
import pandas as pd
import tabula
from io import BytesIO, StringIO
from doc_upload.shared import Q3
from dotenv import load_dotenv
import shutil

load_dotenv()

source = os.environ.get("SOURCE")
s3_bucket = os.environ.get("S3_BUCKET")


COLS = {
    0: "commodity_name",
    1: "contract_month",
    2: "daily_price_range_open#",
    3: "daily_price_range_high",
    4: "daily_price_range_low",
    5: "daily_price_range_close#",
    6: "settle_price",
    7: "settle_change",
    8: "total_volume",
    9: "oi",
    10: "change",
    11: "efp",
    12: "efs",
    13: "block_volume",
    14: "spread_volume",
}


def transform_ice_data(filepath):
    """
    transforms ice data
    """
    df_list = tabula.read_pdf(
        filepath, pages="all", lattice=True, pandas_options={"header": [0, 1]}
    )
    _df = pd.concat(df_list).rename(columns=COLS)
    _df = (
        _df[_df["contract_month"].str.match(r"[A-Za-z]{3}\d{2}") == True]
        .assign(
            contract_month=lambda x: pd.to_datetime(x["contract_month"], format="%b%y")
        )
        .dropna(axis=1)
    )
    return _df


def listfiles(data_dir, date):
    """
    lit files
    """

    filepaths = []
    for root, _, files in os.walk(data_dir):
        for file in files:
            if not file.startswith("."):
                _date = file[4:-4]
                if _date >= date:
                    filepaths.append(os.path.join(root, file))
    filepaths.sort()
    return filepaths


# def write_locally(df, filepath):
#     try:
#         with gzip.open(filepath, "wt") as gz_file:
#             df.to_csv(gz_file, index=False)
#         print(f"Data saved locally: {filepath}")
#     except Exception as e:
#         print(f"Error occurred while writing to {filepath}: {str(e)}")
#         traceback.print_exc()


def write_to_s3(bucket_name, df, filename):
    """
    writes csv files to s3
    """
    q3_client = Q3()
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    gz_buffer = BytesIO()
    with gzip.GzipFile(mode="w", fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(buffer.getvalue(), "utf-8"))

    try:
        obj = q3_client.client.put_object(
            Bucket=bucket_name, Key=filename, Body=gz_buffer.getvalue()
        )
    except Exception as e:
        print(e)
        return None

    return True


def upload_ice_data_in_parallel(data_dir, date, output_dir, bucket_name, prefix):
    """
    processing files
    """
    filepaths = listfiles(data_dir, date)
    print("Filepaths:", filepaths)
    counter = 1
    print(f"Length of the filepath: {len(filepaths)}")

    def process_file(filepath):
        nonlocal counter
        filename = os.path.basename(filepath)
        exchange_code = filename.split("_")[0]
        year = filename.split("_")[1]
        month = filename.split("_")[2]
        day = filename.split("_")[3].replace(".pdf", "")

        output_subdir = os.path.join(output_dir, exchange_code, year, month, day)
        os.makedirs(output_subdir, exist_ok=True)

        df = transform_ice_data(filepath)
        filename = filename.replace("_", "").replace(exchange_code, "").replace("-", "")
        pdf_prefix = os.path.join(
            prefix,
            exchange_code,
            year,
            month,
            day,
            f"{exchange_code}_{filename}",
        )
        csv_prefix = os.path.join(
            prefix,
            exchange_code,
            year,
            month,
            day,
            f"{exchange_code}_{filename.replace('.pdf', '.csv.gz')}",
        )

        # write_locally(df, csv_prefix)
        # write_locally(df, pdf_prefix)
        # print(f"{exchange_code}_{filename.replace('.pdf', '.csv.gz')} saved locally")

        q3_client = Q3()
        q3_client.upload_file(bucket_name, pdf_prefix, filepath)
        write_to_s3(bucket_name, df, csv_prefix)
        print(
            f"{exchange_code}_{filename.replace('.pdf', '.csv.gz')} uploaded to {bucket_name}"
        )

        counter += 1

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_file, filepaths)
    return len(filepaths)


def extract_and_upload_ice_data(startdate, input_file):
    """
    calling all above func so that they can be used in views
    """
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)

    processed_dir = os.path.join(parent_dir, "data", "input")
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    output_dir = os.path.join(parent_dir, "data", "upload")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    bucket_name = s3_bucket
    prefix = source
    num_of_files = 0
    with ZipFile(input_file, "r") as zip_ref:
        zip_ref.extractall(output_dir)
        num_of_files = upload_ice_data_in_parallel(
            processed_dir, startdate, output_dir, bucket_name, prefix
        )
    shutil.rmtree(output_dir)
    return num_of_files
