import itertools
import sys
import time
from datetime import date, timedelta
from multiprocessing import Pool, Manager

import Output
import SingleVAR
from SingleVAR import RunVAR

if __name__ == "__main__":
    tic = time.time()
    Exeptions = []
    # Read date input from command prompt
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
        try:
            DATE = date.fromisoformat(input_date)
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
            sys.exit(1)
    else:
        DATE = date.today() - timedelta(days=0)

    N = int(1e5)
    print("Number of paths:", N)

    Data = SingleVAR.ImportRawData(DATE)
    ledger_list_full = {
        "Prime": [
            "Master",
            "Monthly",
            "Custom1",
            "Quarterly1",
            "MonthlyIG",
            "QuarterlyX",
            "Q364",
            "2YIG",
        ],
        "USG": ["Monthly"],
    }
    StressRunList = ["N", "S", "C"]
    max_processes = 9

    manager = Manager()
    Results = manager.dict()

    pool = Pool(processes=max_processes)

    for Fund in ledger_list_full:
        if Fund == "USG":
            pool.starmap_async(
                RunVAR,
                [
                    (
                        Results,
                        N,
                        Data,
                        Stress,
                        Fund,
                        "Monthly",
                        DATE,
                    )
                    for Stress in StressRunList
                ],
            )
        if Fund == "Prime":
            pool.starmap_async(
                RunVAR,
                [
                    (
                        Results,
                        N,
                        Data,
                        Stress,
                        Fund,
                        ledger,
                        DATE,
                    )
                    for ledger, Stress in itertools.product(
                        ledger_list_full.get("Prime"), StressRunList
                    )
                ],
            )  # type: ignore

    pool.close()
    pool.join()

    print("Uploading Data to SQL...")

    TableName = "VAR_Results"
    Error_list = []
    for Fund in ledger_list_full:
        print(Fund)
        for ledger in ledger_list_full.get(Fund):
            for Stress in StressRunList:
                e = Output.Output_SQL(
                    Results.get((Stress, Fund, ledger)),
                    N,
                    Stress,
                    Fund,
                    ledger,
                    TableName,
                    DATE,
                )
                if e != None:
                    Error_list.append(e)
    if Error_list:
        Exeptions.append(f"SQL UPLOAD FAILED:  {Error_list}")

    if Exeptions:
        print(Exeptions)

    toc = time.time()
    print(
        ">>>>>>>>>>>>>>>>TIME TAKEN",
        int((toc - tic) / 60),
        "min <<<<<<<<<<<<<<<<<<<<<<<<",
    )
