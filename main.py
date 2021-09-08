import yaml
import argparse
import sqlite3
import pandas as pd
import numpy as np
import os

# TODO: add a day, add a date range - so I can get messages from a certain month


def main():
    # read arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--month",
        metavar="month",
        type=str,
        required=True,
        help="a date to pass to the query",
    )
    parser.add_argument(
        "-y",
        "--year",
        metavar="year",
        type=str,
        default="2021",
        help="a date to pass to the query",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="output",
        type=str,
        default="self_messages",
        help="a date to pass to the query",
    )

    args = parser.parse_args()

    month = str(args.month)
    year = str(args.year)
    day = "01"
    output = args.output + ".txt"

    # read config file
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    conn = sqlite3.connect(config["db_path"])
    date = year + "-" + month + "-" + day

    # query message database
    my_messages = pd.read_sql_query(
        f"""
    SELECT 
        DISTINCT text
        , strftime('%Y', datetime(date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime")) AS year
        , strftime('%m', datetime(date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime")) AS month
        , strftime('%d', datetime(date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime")) AS day
        , ROWID
    FROM message
    WHERE handle_id = {config["handle_id"]}
        AND year >= strftime('%Y', '{date}')
        AND month >= strftime('%m', '{date}')
    ORDER BY
        year DESC
        , month DESC
        , day DESC
    """,
        conn,
        index_col="ROWID",
    )

    # remove duplicates
    my_messages.drop_duplicates(subset="text", inplace=True)

    # create output (remove file if it already exists)
    f_path = os.path.join(config["out_path"], output)

    if os.path.exists(f_path):
        os.remove(f_path)

    np.savetxt(f_path, my_messages["text"], fmt="%s")


if __name__ == "__main__":
    main()
