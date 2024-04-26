# pull prices from JPPD and IDC into source file. if AM, gets cusips from helix. if PM, re-prices cusips already there
# (new cusips added throughout the day aren't to be included in PM price)

# JJ Vulopas

import openpyxl as op
import pandas as pd
import requests, base64, http.client, os, subprocess
from datetime import datetime, timedelta
import pymssql
from pymssql import Error
from pathlib import Path, PureWindowsPath
from pandas.tseries.offsets import BDay


# returns tuple of 1) map of fund to all cusips in that fund and 2) pulltime. reads from helix backup file
def old_helix_match():
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

    for f in cusips_all.keys():
        to_add = set()
        for x in cusips_all[f]:
            if len(x) >= 3:
                if x[:3] == "PNI":
                    to_add.add(x[3:len(x)])
        for extra_f in to_add:
            cusips_all[f].add(extra_f)

    return cusips_all, helix_pull_time



# returns tuple of 1) map of fund to all cusips in that fund and 2) pulltime, thru direct SQL query to Helix
def helix_match():
    funds = ["Prime", "USG", "MMT", "LMCP Inv"]
    cusips_all = dict()
    for f in funds:
        cusips_all[f] = set()
        helix_pull_time = datetime.now()
    try:
        print("Fetching CUSIPs from Helix...")
        connection = pymssql.connect(server='LUCIDSQL1',
                                     user='mattiasalmers',
                                     password='12345',
                                     database='HELIXREPO_PROD_02')
        cursor = connection.cursor()
        query =  """
            select distinct
            case when tradepieces.company = 44 then 'USG Fund' when tradepieces.company = 45 then 'Prime Fund' when tradepieces.COMPANY = 46 then 'MMT IM Fund' when tradepieces.COMPANY = 48 then 'LMCP Inv Fund'  when tradepieces.COMPANY = 49 then 'LucidRepo' end Fund,
            ltrim(rtrim(Tradepieces.ISIN)) BondID
            from tradepieces 
            where tradepieces.isvisible = 1
            and tradepieces.company in (44,45,46, 48,49)
            order by Fund ASC
        """
        cursor.execute(query)
        records = cursor.fetchall()
        for row in records:
            if row[0] in ['USG Fund', 'Prime Fund', 'MMT IM Fund','LMCP Inv Fund','LucidRepo']:
                k = row[0][:-5] if row[0] in ['USG Fund', 'Prime Fund'] else 'MMT'
                cusips_all[k].add(row[1])
    except Error as e:
        # if this happens must flag and notify else might wipe cusips from the pricer (unless only clean in the morning)
        print("Error reading data from SQL, reverting to last night backup", e)
        return old_helix_match()
    finally:
        connection.close()
        cursor.close()

    for f in cusips_all.keys():
        to_add = set()
        for x in cusips_all[f]:
            if len(x) >= 3:
                if x[:3] == "PNI":
                    to_add.add(x[3:len(x)])
        for extra_f in to_add:
            cusips_all[f].add(extra_f)
    return cusips_all, helix_pull_time

# fetch all bond data from bloomberg and write to bond data csv
def bb_fetch(cusips):
    print("Fetching data from Bloomberg...")
    # write cusips to file (Sept 2020 replaces passing through command line)
    list_path = "curr_fetch_batch.txt"
    outp = open(list_path,"w") # should overwrite
    for x in cusips:
        outp.write(x+"\n")
    outp.close()
    
    bbpy_path = r"bb_fetch_processor.py"

    try:
        subprocess.check_output("py " + bbpy_path + " " + list_path + " \"Helix\"") # jjv march 2
        #print("py " + bbpy_path + " " + list_path + " \"Helix\"") # jjv march 2
    except:
        print("Error fetching bond data from Bloomberg. Check bb_fetch_processor.py.")

def just_bond_data():
    helix_proc = helix_match()
    pxable_cusips = [c for f in helix_proc[0].keys() for c in helix_proc[0][f]]
    pxable_cusips.append('38178DAA5')
    pxable_cusips = list(dict.fromkeys(pxable_cusips)) # remove duplicates

    bb_fetch(pxable_cusips)
    batch_file = open('BatchLogs.txt', 'a')
    batch_file.write("\n(" + datetime.now().strftime("%a %Y-%m-%d;%H:%M:%S") + ";\"Fetch Bond Data\")") 
    batch_file.close()
    print("All done.")

# main process
if __name__ == "__main__":
    just_bond_data()
