# pull prices from JPPD and IDC into source file. if AM, gets cusips from helix. if PM, re-prices cusips already there
# (new cusips added throughout the day aren't to be included in PM price)

# JJ Vulopas

import openpyxl as op
import pandas as pd
import requests, base64, http.client, os, subprocess
from datetime import datetime, timedelta
from pathlib import Path, PureWindowsPath
from pandas.tseries.offsets import BDay

# returns tuple of 1) map of fund to all cusips in that fund and 2) pulltime
def helix_match():
    print("Fetching CUSIPs from Helix...")
    cusips_all = dict()
    cusips_all["Prime"] = set()
    cusips_all["USG"] = set()
    cusips_all["MMT"] = set()
    cusips_all["LMCP Inv"] = set()
    
    helix_backup_date = (datetime.now() - timedelta(1)).strftime("%m_%d_%Y")
    helix_pull_time = datetime.now()

    for f in cusips_all.keys():
        helix_backup_file = PureWindowsPath(
            Path("S:/Mandates/Operations/Helix Trade Files/" + f + (" Fund" if f != "LMCP Inv" else "") + ".txt"))
        fund_df = pd.read_csv(helix_backup_file, sep="\t")
        for c in fund_df["BondID"]: # changed 10/3/2019 from "Cusip" to "BondID"
            if isinstance(c, str):
                if c.strip().isalnum(): # added strip 11/26 in case errant spaces
                    cusips_all[f].add(c)
        for extra_mmt in ['XS1951177309','SOSPRUCE1','SOSPRUCE2','XS2091648928','EASTCYPR1']:
        	cusips_all["MMT"].add(extra_mmt)

    return cusips_all, helix_pull_time

# fetch all bond data from bloomberg and write to bond data csv
def bb_fetch(cusips):
    print("Fetching data from Bloomberg...")
    cusipstr = ",".join(cusips)
    bbpy_path = r"bb_fetch_processor.py"
    try:
        subprocess.check_output("py " + bbpy_path + " \"" + cusipstr + "\"" + " \"Helix\"") # jjv march 2
    except:
        print("Error fetching bond data from Bloomberg. Check bb_fetch_processor.py.")

def just_bond_data():
    helix_proc = helix_match()
    pxable_cusips = [c for f in helix_proc[0].keys() for c in helix_proc[0][f]]
    pxable_cusips = list(dict.fromkeys(pxable_cusips)) # remove duplicates
    bb_fetch(pxable_cusips)
    print("All done.")

# main process
if __name__ == "__main__":
    just_bond_data()