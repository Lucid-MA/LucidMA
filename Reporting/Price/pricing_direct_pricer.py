# JJ Vulopas
# only works in 2.7
# cmd args are date and then cusips to price, separated by spaces, assumes date is a business day with a bond close
import requests
import sys
import base64
import ast
import datetime as dt

# return cusips and associated bid prices
def pricing_direct_request(cusips, vdate):
    url = 'https://www.pricing-direct.com/pricingdirect/request/priceWsFICusips'
    data = {
             "Cusip": cusips,
             "Date": [vdate.strftime("%m/%d/%Y")],
             "CloseType": [ "BOND" ],
             "PriceType": [ "BID" ]}

    namepass = "yating:19960601Lyt" # This password will periodically expire. Go to https://www.pricing-direct.com/pricingdirect/ > Login > "Forgot Password" to reset.
    headers_pd = {"Authorization":"Basic %s" %base64.b64encode(namepass.encode("utf_8"))}

    response = requests.post(url, json=data, headers=headers_pd)
    src = ast.literal_eval(response.text)
    for i in range(0, len(src) - 1): # skip disclaimer, the last
        x = src[i]
        print(x["SecurityID"] + ", " + x['Bid Evaluation'])

if __name__ == "__main__":
    vdate = dt.datetime.strptime(sys.argv[1], "%Y%m%d")
    cusips = []
    for i in range(2, len(sys.argv)):
        cusips.append(sys.argv[i])
    pricing_direct_request(cusips, vdate=vdate)
