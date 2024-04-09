import os
import argparse
import gzip
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import tabula
from zipfile import ZipFile
from io import BytesIO, StringIO

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

SOURCE = "IFED"
# S3_BUCKET = "enine-test"


def transform_ice_data(filepath):
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
#     with gzip.open(filepath, "wt") as gz_file:
#         df.to_csv(gz_file, index=False)
#     print(f"Data saved locally: {filepath}")


import traceback

def write_locally(df, filepath):
    try:
        with gzip.open(filepath, "wt") as gz_file:
            df.to_csv(gz_file, index=False)
        print(f"Data saved locally: {filepath}")
    except Exception as e:
        print(f"Error occurred while writing to {filepath}: {str(e)}")
        traceback.print_exc()


def upload_ice_data_locally(data_dir, date, output_dir):
    filepaths = listfiles(data_dir, date)
    print(filepaths)
    counter = 1
    print(f"Length of the filepath: {len(filepaths)}")

    def process_file(filepath):
        nonlocal counter
        print(f"Processing {counter} of {len(filepaths)}")
        filename = os.path.basename(filepath)
        exchange_code = filename.split("_")[0]
        year = filename.split("_")[1]
        month = filename.split("_")[2]
        day = filename.split("_")[3].replace(".pdf", "")
        df = transform_ice_data(filepath)
        filename = filename.replace("_", "").replace(exchange_code, "").replace("-", "")
        pdf_prefix = os.path.join(
            output_dir,
            exchange_code,
            year,
            month,
            day,
            f"{exchange_code}_{filename}",
        )
        csv_prefix = os.path.join(
            output_dir,
            exchange_code,
            year,
            month,
            day,
            f"{exchange_code}_{filename.replace('.pdf', '.csv.gz')}",
        )


        write_locally(df, csv_prefix)
        print(f"{exchange_code}_{filename.replace('.pdf', '.csv.gz')} saved locally")
        counter += 1

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_file, filepaths)

def extract_and_upload_ice_data(input_file):
    startdate = "2023_10_01"
    processed_dir="/home/fox/developer/automate_upload/data/temp"
    output_dir = "/home/fox/developer/automate_upload/data/upload"
    with ZipFile(input_file, "r") as zip_ref:
        zip_ref.extractall(output_dir)
        upload_ice_data_locally(processed_dir, startdate, output_dir)


