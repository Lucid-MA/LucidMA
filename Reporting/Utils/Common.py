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
