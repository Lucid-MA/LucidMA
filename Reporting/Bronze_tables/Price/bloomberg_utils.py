import logging
from datetime import datetime
from functools import wraps
from typing import Dict, List, Any, Optional

import blpapi
import pandas as pd

from Utils.Common import get_current_date, get_current_timestamp
from Utils.Constants import benchmark_ticker

special_cusips = [
    {
        "CUSIP": "CASHUSD01",
        "SECURITY_TYP": "CASH",
        "ISSUER": "US TREASURY",
        "Collat Typ": "Cash",
        "Name": "Cash",
        "Industry Sector": "USD Cash",
        "Issue DT": "7/4/1776",
        "Maturity": "12/31/2099",
        "Amt Outstanding": "1",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "0",
        "YAS_MOD_DUR": "0",
        "USED DURATION": "0.00273972",
        "Days Acc": "0",
        "YLD_ytm_BID": "0",
        "I_SPRD_BID": "0",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "0",
        "MTG TRANCHE TYP LONG": "CASH",
        "MTG PL CPR 1M": "0",
        "MTG PL CPR 6M": "0",
        "MTG_WHLN_GEO1": "0",
        "MTG_WHLN_GEO2": "0",
        "MTG_WHLN_GEO3": "0",
        "RATINGS BUCKET": "USG",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_EGAN_JONES": "",
        "DELIVERY_TYP": "",
        "Est'd Asset Class": "CASH",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "1",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPCASHUSD",
        "SECURITY_TYP": "CASH",
        "ISSUER": "US TREASURY",
        "Collat Typ": "Cash",
        "Name": "Cash",
        "Industry Sector": "USD Cash",
        "Issue DT": "7/4/1776",
        "Maturity": "12/31/2099",
        "Amt Outstanding": "1",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "0",
        "YAS_MOD_DUR": "0",
        "USED DURATION": "0.00273972",
        "Days Acc": "0",
        "YLD_ytm_BID": "0",
        "I_SPRD_BID": "0",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "0",
        "MTG TRANCHE TYP LONG": "CASH",
        "MTG PL CPR 1M": "0",
        "MTG PL CPR 6M": "0",
        "MTG_WHLN_GEO1": "0",
        "MTG_WHLN_GEO2": "0",
        "MTG_WHLN_GEO3": "0",
        "RATINGS BUCKET": "USG",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_EGAN_JONES": "",
        "DELIVERY_TYP": "",
        "Est'd Asset Class": "CASH",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "1",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "CASHEUR01",
        "SECURITY_TYP": "CASH",
        "ISSUER": "ECB",
        "Collat Typ": "Cash",
        "Name": "Cash",
        "Industry Sector": "EUR Cash",
        "Issue DT": "1/1/1999",
        "Maturity": "12/31/2099",
        "Amt Outstanding": "1",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "0",
        "YAS_MOD_DUR": "0",
        "USED DURATION": "0.00273972",
        "Days Acc": "0",
        "YLD_ytm_BID": "0",
        "I_SPRD_BID": "0",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "0",
        "MTG TRANCHE TYP LONG": "CASH",
        "MTG PL CPR 1M": "0",
        "MTG PL CPR 6M": "0",
        "MTG_WHLN_GEO1": "0",
        "MTG_WHLN_GEO2": "0",
        "MTG_WHLN_GEO3": "0",
        "RATINGS BUCKET": "USG",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_EGAN_JONES": "",
        "DELIVERY_TYP": "",
        "Est'd Asset Class": "CASH",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "1",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "74039RAA8",
        "SECURITY_TYP": "",
        "ISSUER": "Preeti Trust SWMC 2021",
        "Collat Typ": "",
        "Name": "",
        "Industry Sector": "",
        "Issue DT": "6/1/2021",
        "Maturity": "12/31/2021",
        "Amt Outstanding": "50000000",
        "Coupon": "2.96",
        "Floater": "",
        "MTG Factor": "1",
        "PX Bid": "101.5",
        "PX Mid": "101.5",
        "Int Acc": "0",
        "Mtg WAL": "5.2",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "USG",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "MBSTRUST",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "371494AK1",
        "SECURITY_TYP": "",
        "ISSUER": "Tri Party IGCorp",
        "Collat Typ": "",
        "Name": "Tri Party IGCorp",
        "Industry Sector": "",
        "Issue DT": "8/10/2021",
        "Maturity": "8/10/2031",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "5",
        "DUR ADJ OAS BID": "5",
        "YAS_MOD_DUR": "5",
        "USED DURATION": "5",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "TRIPTYIGCORP",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "2063C0VL3",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "Concord",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "A1/P1",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "MMFCP",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-TH2O",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-TH2O",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-SCHF1",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-SCHF1",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-DYM1",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-DYM1",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-4631",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-4631",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-PBTA1",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-PBTA1",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-SVI1",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-SVI1",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
    {
        "CUSIP": "JPM-MANT1",
        "SECURITY_TYP": "",
        "ISSUER": "Concord",
        "Collat Typ": "",
        "Name": "JPM-MANT1",
        "Industry Sector": "",
        "Issue DT": "8/13/2021",
        "Maturity": "8/20/2021",
        "Amt Outstanding": "10000000",
        "Coupon": "0",
        "Floater": "N",
        "MTG Factor": "1",
        "PX Bid": "100",
        "PX Mid": "100",
        "Int Acc": "0",
        "Mtg WAL": "0",
        "DUR ADJ OAS BID": "",
        "YAS_MOD_DUR": "",
        "USED DURATION": "",
        "Days Acc": "",
        "YLD_ytm_BID": "",
        "I_SPRD_BID": "",
        "FLT_SPREAD": "",
        "OAS_SPREAD_ASK": "",
        "MTG TRANCHE TYP LONG": "",
        "MTG PL CPR 1M": "",
        "MTG PL CPR 6M": "",
        "MTG_WHLN_GEO1": "",
        "MTG_WHLN_GEO2": "",
        "MTG_WHLN_GEO3": "",
        "RATINGS BUCKET": "BBB",
        "RTG_SP": "",
        "RTG_MOODY": "",
        "RTG_FITCH": "",
        "RTG_KBRA": "",
        "RTG_DBRS": "",
        "RTG_Egan_Jones": "",
        "DELIVERY_TYP": "PHYS",
        "Est'd Asset Class": "FUNDINT",
        "CUSIP or ISIN": " CUSIP",
        "MTG_PREV_FACTOR": "",
        "MTG_RECORD_DT": "",
        "MTG_FACTOR_PAY_DT": "",
        "MTG_NXT_PAY_DT_SET_DT": "",
    },
]

diff_cusip_map = {
    "EURFX": "EUR Curncy",
    "XS2606220999": "PPG91FR41",
    "XS2592024009": "PPGA0OKN5",
    "XS2592025071": "PPG80PFP8",
    "ALMNDUSD7": "PPFM1GA98",
    "ALMNDUSD8": "PPFR5TTD6",
    "STHAPPLE5": "PPFT6V9U0",
    "OPPORTUN1": "PPFQKMXT6",
    "STHAPPLE4": "PPF54YY06",
    "MNTNCHRY1": "PPEBEGFI4",
    "MNTNCHRY2": "PPFX3C1P5",
    "52953BBJ1": "PPEG3JY56",
    "STHAPPLE2": "PPED2BZX9",
    "XS2373029664": "PPEZDK875",
    "52468JX82": "PPE43E6K2",
    "XS2373029748": "PPE0FKE65",
    "STHAPPLE1": "PPE32P4N6",
    "HEXZETA01": "PPE9DMNR8",
    "HEXZETA02": "PPE4DGC45",
    "STHAPPLE3": "PPE939O30",
    "ALMNDUSD4": "PPE0F22A9",
    "ALMNDUSD5": "PPEA34F46",
    "ALMNDUSD6": "PPEBFPHO8",
    "ALMNDEUR4": "PPE5GG8O0",
    "ALMNDEUR3": "PPEA34F53",
    "ALMNDUSD3": "PPE139546",
    "ALMNDUSD2": "PPEE2UU10",
    "ALMNDUSD1": "PPE32P4O4",
    "ALMNDEUR2": "PPE42XYF1",
    "ALMNDEUR1": "PPEA2SM53",
    "XS2225938831": "PPEF1MMY3",
    "XS2091648928": "PP9FCKDZ8",
    "XS1951177309": "PP30JD700",
    "XS2004377136": "PP075HWJ5",
    "XS2643730695": "PPG64I278",
    "XS2644211281": "PPG24HYG4",
    "XS2644210986": "PPG64I468",
    "XS2644211109": "PPG64I4G6",
    "TREATYUS1": "PPG1JQ3D1",
    "XS2643730695": "PPG1K4CZ9",
    "XS2644210986": "PPG5K61F1",
    "XS2644211109": "PPG1K4CY2",
    "XS2644211281": "PPG5K61D6",
}

bb_fields = [
    "SECURITY_TYP",
    "ISSUER",
    "COLLAT_TYP",
    "NAME",
    "INDUSTRY_SECTOR",
    "ISSUE_DT",
    "MATURITY",
    "AMT_OUTSTANDING",
    "COUPON",
    "FLOATER",
    "MTG_FACTOR",
    "PX_BID",
    "PX_MID",
    "INT_ACC",
    "MTG_WAL",
    "MTG_ORIG_WAL",
    "DUR_ADJ_OAS_BID",
    "YAS_MOD_DUR",
    "DAYS_ACC",
    "YLD_YTM_BID",
    "I_SPRD_BID",
    "FLT_SPREAD",
    "OAS_SPREAD_ASK",
    "MTG_TRANCHE_TYP_LONG",
    "MTG_PL_CPR_1M",
    "MTG_PL_CPR_6M",
    "MTG_WHLN_GEO1",
    "MTG_WHLN_GEO2",
    "MTG_WHLN_GEO3",
    "RTG_SP",
    "RTG_MOODY",
    "RTG_FITCH",
    "RTG_KBRA",
    "RTG_DBRS",
    "RTG_EGAN_JONES",
    "DELIVERY_TYP",
    "DTC_REGISTERED",
    "DTC_ELIGIBLE",
    "MTG_DTC_TYP",
    "TRADE_DT_ACC_INT",
    "PRINCIPAL_FACTOR",
    "MTG_PREV_FACTOR",
    "MTG_RECORD_DT",
    "MTG_FACTOR_PAY_DT",
    "MTG_NXT_PAY_DT_SET_DT",
    "IDX_RATIO",
]

bb_fields_selected = [
    "SECURITY_TYP",
    "ISSUER",
    "COLLAT_TYP",
    "NAME",
    "INDUSTRY_SECTOR",
    "ISSUE_DT",
    "MATURITY",
    "PX_LAST",
    "AMT_OUTSTANDING",
    "COUPON",
    "FLOATER",
    "MTG_FACTOR",
    "INT_ACC",
    "MTG_WAL",
    "DAYS_ACC",
    "RTG_SP",
    "RTG_MOODY",
    "RTG_FITCH",
    "RTG_KBRA",
    "RTG_DBRS",
    "RTG_EGAN_JONES",
    "DELIVERY_TYP",
    "DTC_REGISTERED",
    "DTC_ELIGIBLE",
    "MTG_DTC_TYP",
    "PRINCIPAL_FACTOR",
    "MTG_PREV_FACTOR",
    "MTG_RECORD_DT",
    "MTG_FACTOR_PAY_DT",
    "MTG_NXT_PAY_DT_SET_DT",
    "IDX_RATIO",
]

bb_cols_selected = [
    "security_type",
    "issuer",
    "collateral_type",
    "name",
    "industry_sector",
    "issue_date",
    "maturity",
    "price",
    "amt_outstanding",
    "coupon",
    "floater",
    "mtg_factor",
    "interest_accrued",
    "mtg_wal",
    "days_accrual",
    "rtg_sp",
    "rtg_moody",
    "rtg_fitch",
    "rtg_kbra",
    "rtg_dbrs",
    "rtg_egan_jones",
    "delivery_type",
    "dtc_registered",
    "dtc_eligible",
    "mtg_dtc_type",
    "principal_factor",
    "mtg_prev_factor",
    "mtg_record_date",
    "mtg_factor_pay_date",
    "mtg_next_pay_date_set_date",
    "idx_ratio",
]

bb_cols = [
    "CUSIP",
    "SECURITY_TYP",
    "ISSUER",
    "Collat Typ",
    "Name",
    "Industry Sector",
    "Issue DT",
    "Maturity",
    "Amt Outstanding",
    "Coupon",
    "Floater",
    "MTG Factor",
    "PX Bid",
    "PX Mid",
    "Int Acc",
    "Mtg WAL",
    "DUR ADJ OAS BID",
    "YAS_MOD_DUR",
    "USED DURATION",
    "Days Acc",
    "YLD_ytm_BID",
    "I_SPRD_BID",
    "FLT_SPREAD",
    "OAS_SPREAD_ASK",
    "MTG TRANCHE TYP LONG",
    "MTG PL CPR 1M",
    "MTG PL CPR 6M",
    "MTG_WHLN_GEO1",
    "MTG_WHLN_GEO2",
    "MTG_WHLN_GEO3",
    "RATINGS BUCKET",
    "RTG_SP",
    "RTG_MOODY",
    "RTG_FITCH",
    "RTG_KBRA",
    "RTG_DBRS",
    "RTG_EGAN_JONES",
    "DELIVERY_TYP",
    "Est'd Asset Class",
    "CUSIP or ISIN",
    "MTG_PREV_FACTOR",
    "MTG_RECORD_DT",
    "MTG_FACTOR_PAY_DT",
    "MTG_NXT_PAY_DT_SET_DT",
    "IDX_RATIO",
]


class BloombergDataFetcher:
    def __init__(
        self, host: str = "localhost", port: int = 8194, missing_value: Any = None
    ):
        self.session_options = blpapi.SessionOptions()
        self.session_options.setServerHost(host)
        self.session_options.setServerPort(port)
        self.missing_value = missing_value

    @staticmethod
    def _session_wrapper(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            session = None
            try:
                session = blpapi.Session(self.session_options)
                if not session.start():
                    logging.error("Failed to start session.")
                    return pd.DataFrame()
                if not session.openService("//blp/refdata"):
                    logging.error("Failed to open //blp/refdata")
                    return pd.DataFrame()
                return func(self, session, *args, **kwargs)
            except blpapi.Exception as e:
                logging.error(f"Bloomberg API exception: {e}")
                return pd.DataFrame()
            finally:
                if session:
                    session.stop()

        return wrapper

    @staticmethod
    def _prepare_security(security: str) -> str:
        if len(security) == 9:
            prefix = "/cusip/"
        elif security in ("3137F8RH8", "3137F8ZC0"):
            prefix = "/mtge/"
        else:
            prefix = "/isin/"
        return prefix + security

    @staticmethod
    def _remove_prefix(text: str, prefixes: List[str]) -> str:
        for prefix in prefixes:
            if text.startswith(prefix):
                return text[len(prefix) :]
        return text

    def _send_request_and_get_data(
        self, session: blpapi.Session, request: blpapi.Request, timeout: int = 5000
    ) -> List[Dict[str, Any]]:
        session.sendRequest(request)
        data = []
        retries = 3
        while retries > 0:
            event = session.nextEvent(timeout)
            if event.eventType() in [
                blpapi.Event.RESPONSE,
                blpapi.Event.PARTIAL_RESPONSE,
            ]:
                for msg in event:
                    data.extend(self._process_message(msg))
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            elif event.eventType() == blpapi.Event.TIMEOUT:
                logging.warning(
                    "Timeout occurred while waiting for response. Retrying..."
                )
                retries -= 1
                if retries == 0:
                    logging.error("Maximum retries reached. Returning partial data.")
                    break
        return data

    def _process_message(self, msg: blpapi.Message) -> List[Dict[str, Any]]:
        data = []
        if msg.messageType() == "ReferenceDataResponse":
            security_data = msg.getElement("securityData")
            for i in range(security_data.numValues()):
                security = security_data.getValueAsElement(i)
                ticker = self._remove_prefix(
                    security.getElementAsString("security"), ["/cusip/", "/isin/"]
                )
                field_data = security.getElement("fieldData")
                row = {"security": benchmark_ticker.get(ticker, ticker)}
                for field in field_data.elements():
                    field_name = str(field.name())  # Convert blpapi.Name to string
                    if field.numValues() > 0:
                        row[field_name] = field.getValue()
                    else:
                        row[field_name] = self.missing_value
                data.append(row)
        elif msg.messageType() == "HistoricalDataResponse":
            security_data = msg.getElement("securityData")
            security = security_data.getElementAsString("security")
            security = self._remove_prefix(security, ["/cusip/", "/isin/"])
            if not security_data.hasElement("securityError"):
                field_data_array = security_data.getElement("fieldData")
                for field_data in field_data_array.values():
                    row = {"security": benchmark_ticker.get(security, security)}
                    for field in field_data.elements():
                        field_name = str(field.name())  # Convert blpapi.Name to string
                        if field.numValues() > 0:
                            row[field_name] = field.getValue()
                        else:
                            row[field_name] = self.missing_value
                    data.append(row)
            else:
                error_msg = security_data.getElement("securityError")
                logging.error(f"Security error for {security}: {error_msg}")
        return data

    @_session_wrapper
    def get_latest_prices(
        self, session: blpapi.Session, securities: List[str]
    ) -> pd.DataFrame:
        service = session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")

        for security in securities:
            request.getElement("securities").appendValue(
                self._prepare_security(security)
            )
        request.getElement("fields").appendValue("PX_LAST")

        data = self._send_request_and_get_data(session, request)

        benchmark_date = get_current_date()
        timestamp = get_current_timestamp()

        prices = {item["security"]: item["PX_LAST"] for item in data}
        result = {
            "benchmark_date": benchmark_date,
            "timestamp": timestamp,
            **prices,
        }

        return pd.DataFrame([result])

    @_session_wrapper
    def get_benchmark_historical_prices(
        self,
        session: blpapi.Session,
        securities: List[str],
        start_date: str,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        service = session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")

        for security in securities:
            request.getElement("securities").appendValue(
                self._prepare_security(security)
            )
        request.getElement("fields").appendValue("PX_LAST")
        request.set("startDate", start_date)
        request.set("endDate", end_date or start_date)

        raw_data = self._send_request_and_get_data(session, request)

        # Process the raw data to ensure all fields are present
        processed_data = []
        for item in raw_data:
            processed_item = {"security": item["security"], "date": item["date"]}
            processed_item["PX_LAST"] = item.get("PX_LAST", self.missing_value)
            processed_data.append(processed_item)

        df = pd.DataFrame(processed_data)

        # Pivot the DataFrame to achieve the desired format
        df_pivot = df.pivot(index="date", columns="security", values="PX_LAST")
        df_pivot.columns.name = None

        # Reorder the columns based on the specified order
        column_order = [
            "1m SOFR",
            "3m SOFR",
            "6m SOFR",
            "1y SOFR",
            "1m LIBOR",
            "3m LIBOR",
            "1m A1/P1 CP",
            "3m A1/P1 CP",
            "6m A1/P1 CP",
            "9m A1/P1 CP",
            "1m T-Bill",
            "3m T-Bill",
        ]
        df_pivot = df_pivot.reindex(columns=column_order)

        # Reset the index to turn 'date' into a regular column
        df_pivot.reset_index(inplace=True)

        return df_pivot

    @_session_wrapper
    def get_benchmark_security_attributes(
        self, session: blpapi.Session, securities: List[str], fields: List[str]
    ) -> pd.DataFrame:
        service = session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")

        for security in securities:
            request.getElement("securities").appendValue(
                self._prepare_security(security)
            )
        for field in fields:
            request.getElement("fields").appendValue(field)

        raw_data = self._send_request_and_get_data(session, request)

        # Process the raw data to ensure all fields are present
        processed_data = {}
        for item in raw_data:
            security = item["security"]
            processed_data[security] = item.get("PX_LAST", self.missing_value)
            if security in ["1m T-Bill", "3m T-Bill"]:
                processed_data[f"{security} Maturity"] = item.get(
                    "MATURITY", self.missing_value
                )

        # Create a DataFrame from the processed data
        df = pd.DataFrame([processed_data])

        # Reorder the columns based on the specified order
        column_order = [
            "1m SOFR",
            "3m SOFR",
            "6m SOFR",
            "1y SOFR",
            "1m LIBOR",
            "3m LIBOR",
            "1m A1/P1 CP",
            "3m A1/P1 CP",
            "6m A1/P1 CP",
            "9m A1/P1 CP",
            "1m T-Bill",
            "1m T-Bill Maturity",
            "3m T-Bill",
            "3m T-Bill Maturity",
        ]
        df = df.reindex(columns=column_order)

        return df

    @_session_wrapper
    def get_historical_benchmark_attributes(
        self,
        session: blpapi.Session,
        securities: List[str],
        start_date: str,
        fields: List[str],
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        service = session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")

        for security in securities:
            request.getElement("securities").appendValue(
                self._prepare_security(security)
            )
        for field in fields:
            request.getElement("fields").appendValue(field)

        request.set("startDate", start_date)
        request.set("endDate", end_date or start_date)

        raw_data = self._send_request_and_get_data(session, request)

        # Process the raw data to ensure all fields are present
        processed_data = []
        for item in raw_data:
            processed_item = {"security": item["security"], "date": item["date"]}
            for field in fields:
                processed_item[field] = item.get(field, self.missing_value)
            processed_data.append(processed_item)

        df = pd.DataFrame(processed_data)

        # Pivot the DataFrame to achieve the desired format
        df_pivot = df.pivot(index="date", columns="security", values="PX_LAST")
        df_pivot.columns.name = None

        # Rename the '1m T-Bill' and '3m T-Bill' columns to include 'Maturity'
        maturity_columns = {}
        for col in ["1m T-Bill", "3m T-Bill"]:
            if col in df_pivot.columns:
                maturity_columns[col] = df[df["security"] == col].set_index("date")[
                    "MATURITY"
                ]

        for col, maturity_col in maturity_columns.items():
            df_pivot[f"{col} Maturity"] = maturity_col

        # Reorder the columns based on the specified order
        column_order = [
            "1m SOFR",
            "3m SOFR",
            "6m SOFR",
            "1y SOFR",
            "1m LIBOR",
            "3m LIBOR",
            "1m A1/P1 CP",
            "3m A1/P1 CP",
            "6m A1/P1 CP",
            "9m A1/P1 CP",
            "1m T-Bill",
            "1m T-Bill Maturity",
            "3m T-Bill",
            "3m T-Bill Maturity",
        ]
        df_pivot = df_pivot.reindex(columns=column_order)

        # Reset the index to turn 'date' into a regular column
        df_pivot.reset_index(inplace=True)

        return df_pivot

    ## TEST OUT FACTOR AND ACCRUED INTEREST

    @_session_wrapper
    def get_security_attributes(
        self, session: blpapi.Session, securities: List[str], fields: List[str]
    ) -> pd.DataFrame:
        service = session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")

        for security in securities:
            request.getElement("securities").appendValue(
                self._prepare_security(security)
            )
        for field in fields:
            request.getElement("fields").appendValue(field)

        raw_data = self._send_request_and_get_data(session, request)

        # Process the raw data to ensure all fields are present
        processed_data = []
        for item in raw_data:
            processed_item = {"security": item["security"]}
            for field in fields:
                processed_item[field] = item.get(field, self.missing_value)
            processed_data.append(processed_item)

        # Create a DataFrame from the processed data
        df = pd.DataFrame(processed_data)

        return df

    @_session_wrapper
    def get_historical_security_attributes(
        self,
        session: blpapi.Session,
        securities: List[str],
        start_date: str,
        fields: List[str],
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        service = session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")

        for security in securities:
            request.getElement("securities").appendValue(
                self._prepare_security(security)
            )
        for field in fields:
            request.getElement("fields").appendValue(field)

        request.set("startDate", start_date)
        request.set("endDate", end_date or start_date)

        raw_data = self._send_request_and_get_data(session, request)

        # Process the raw data to ensure all fields are present
        processed_data = []
        for item in raw_data:
            processed_item = {"security": item["security"], "date": item["date"]}
            for field in fields:
                processed_item[field] = item.get(field, self.missing_value)
            processed_data.append(processed_item)

        df = pd.DataFrame(processed_data)

        return df
