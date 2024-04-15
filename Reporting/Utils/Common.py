import sys

import pandas as pd
import platform


def print_df(df):
    with pd.option_context(
            "display.max_columns",
            None,
            "display.width",
            1000,
            "display.float_format",
            "{:.4f}".format,
    ):
        print(df)


def redirect_output_to_log_file(log_file_path):
    sys.stdout = open(log_file_path, 'w')
    sys.stderr = sys.stdout


def get_file_path(file_path_windows):
    if platform.system() == 'Darwin':  # macOS
        file_path = file_path_windows.replace("S:", "/Volumes/Sdrive$")
    elif platform.system() == 'Windows':
        file_path = file_path_windows
    else:
        raise Exception('Unsupported platform')
    return file_path
