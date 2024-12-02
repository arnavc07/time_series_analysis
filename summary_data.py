import pandas as pd

import sys

sys.path.append("/Users/arnav/personal_code/time_series_analysis/")
from sources import PolygonIo

import logging
import argparse

logging.basicConfig(
    filename="equities_summary_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def update_summary_data(update_prod=False):
    "simple function to write down updated summary data for US stocks daily"

    todays_date = pd.Timestamp.today()
    end_date = todays_date + pd.DateOffset(days=1)

    fname = "/Users/arnav/personal_code/data/us_stocks_daily_summary.parquet"

    existing_data = pd.read_parquet(fname)

    volume_params = {
        "start_date": todays_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }

    logging.info(f"Updating equities summary data for {todays_date} from Polygon.io")

    poly = PolygonIo()

    try:
        updated_data = poly.get_end_of_day_stock_summary(
            **volume_params, return_raw_response=False
        )
        all_data = pd.concat(
            [existing_data, updated_data], axis="rows", ignore_index=True
        )

        if update_prod:
            all_data.to_parquet(fname)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Options for updating equities summary data")
    parser.add_argument(
        "--update_prod",
        action="store_true",
        help="Update the production data with the new data",
    )

    args = parser.parse_args()
    update_summary_data(update_rpod=args.update_prod)