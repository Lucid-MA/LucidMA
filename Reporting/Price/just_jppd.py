# pull prices from JPPD and IDC into source file. if AM, gets cusips from helix. if PM, re-prices cusips already there
# (new cusips added throughout the day aren't to be included in PM price)

# JJ Vulopas

import requests, base64, http.client, os, subprocess
from datetime import datetime
from pathlib import Path, PureWindowsPath
from pandas.tseries.offsets import BDay
import sys

if __name__ == "__main__":
    py27_pd_path = r"pricing_direct_pricer.py"
    pd_cusip_req = ""

    try :
        cusipstr = sys.argv[1]
    except:
        print("Please enter at least one valid cusip into command line (or multiple separated by commas). Exiting...")
        quit()

    cusipstr = cusipstr.upper().strip()
    cusips = cusipstr.split()
    cusips = [x.strip() for x in cusips]

    for c in cusips:
        pd_cusip_req = pd_cusip_req + " " + c

    pddate = datetime.now()
    pddate = pddate if pddate.hour >= 16 and pddate.minute >= 30 else (pddate - BDay(1))
    pd_response = subprocess.check_output("py -2.7 " + py27_pd_path + " " + pddate.strftime("%Y%m%d") + " " + pd_cusip_req)
    pd_response_time = datetime.now()
    pd_prices = dict()

    for p in pd_response.decode("utf-8").splitlines():
        cusip, px = p.split(",")
        try :
            print(cusip + "     " + str(px))
        except ValueError:
            continue
    
