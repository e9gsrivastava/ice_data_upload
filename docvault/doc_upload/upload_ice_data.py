import argparse
import gzip
import os
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO

import pandas as pd
import tabula
from q3 import Q3

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
S3_BUCKET = "enine-test"


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
                    # print(_date)
                    filepaths.append(os.path.join(root, file))
    filepaths.sort()
    return filepaths


def write_to_s3(bucket_name, df, filename):
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

def upload_ice_data_in_parallel(data_dir, bucket, prefix, date):
    filepaths = listfiles(data_dir, date)
    print(filepaths)
    # return
    q3_client = Q3()
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

        print(f" csv prefix -- {csv_prefix}")
        print(f"pdf prefix -- {pdf_prefix}")
        # upload pdf to s3
        q3_client.upload_file(bucket, pdf_prefix, filepath)

        # upload df to s3
        write_to_s3(bucket, df, csv_prefix)

        print(
            f"{exchange_code}_{filename.replace('.pdf', '.csv.gz') } uploaded to {bucket}"
        )
        counter += 1

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_file, filepaths)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "startdate",
        nargs="+",
        help="start date to upload ice files eg: 2023_10_01",
    )
    args = parser.parse_args()
    print(args.startdate[0])
    upload_ice_data_in_parallel(os.getcwd(), S3_BUCKET, SOURCE, args.startdate[0])
