import sys

import pandas as pd


def print_df(df):
    with pd.option_context(
            "display.max_columns",
            None,
            "display.width",
            1000,
            "display.float_format",
            "{:.2f}".format,
    ):
        print(df)


def redirect_output_to_log_file(log_file_path):
    sys.stdout = open(log_file_path, 'w')
    sys.stderr = sys.stdout
