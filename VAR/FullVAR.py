import itertools
import logging
import sys
import time
from datetime import date
from multiprocessing import Pool, Manager

import Output
import SingleVAR
from SingleVAR import RunVAR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


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
        DATE = date.today()

    N = int(1e5)
    print("Number of paths:", N)

    Data = SingleVAR.ImportRawData(DATE)

    # ledger_list_full = {'Prime':['Master','Monthly','Custom1','Quarterly1','MonthlyIG', 'QuarterlyX','Q364','2YIG'],'USG':['Monthly']}
    # StressRunList = ["N", "S", "C"]
    # TODO: replace this
    ledger_list_full = {"Prime": ["Master"]}
    StressRunList = ["N"]
    max_processes = 8

    manager = Manager()
    Results = manager.dict()

    pool = Pool(processes=max_processes)

    results = []  # Store the async result objects

    for Fund in ledger_list_full:
        if Fund == "USG":
            result = pool.starmap_async(
                RunVAR,
                [
                    (Results, N, Data, Stress, Fund, "Monthly", DATE)
                    for Stress in StressRunList
                ],
            )
            results.append(result)
        if Fund == "Prime":
            result = pool.starmap_async(
                RunVAR,
                [
                    (Results, N, Data, Stress, Fund, ledger, DATE)
                    for ledger, Stress in itertools.product(
                        ledger_list_full.get("Prime"), StressRunList
                    )
                ],
            )  # type: ignore
            results.append(result)

    # Close the pool to prevent any more tasks from being submitted
    pool.close()

    # Ensure that all async processes complete by calling get() on each
    for result in results:
        result.get()  # This will block until all tasks are completed

    # Join the pool to clean up the processes
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

    # outlook = win32.gencache.EnsureDispatch('Outlook.Application') # type: ignore
    # new_mail = outlook.CreateItem(0)
    # new_mail.To = 'Yating.Liu@lucidma.com'
    # today=date.today()
    # if Exeptions:
    #     new_mail.Subject = f'! VAR ISSUE ! - {today}'
    #     body = f"<div><div>{Exeptions}</div></div>"
    #     new_mail.HTMLBody = (body)
    #     new_mail.Send()

    toc = time.time()
    print(
        ">>>>>>>>>>>>>>>>TIME TAKEN",
        int((toc - tic) / 60),
        "min <<<<<<<<<<<<<<<<<<<<<<<<",
    )
