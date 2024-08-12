import blpapi
from typing import List, Dict

import pandas as pd
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    String,
    Float,
    DateTime,
    MetaData,
    text,
)
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Constants import benchmark_ticker
from Utils.SQL_queries import daily_price_securities_helix_query
from Utils.database_utils import (
    engine_prod,
    get_database_engine,
    execute_sql_query,
    helix_db_type,
)

PUBLISH_TO_PROD = True
tb_name = "bronze_benchmark"


class BloombergDataFetcher:
    def __init__(self, host: str = "localhost", port: int = 8194):
        self.session_options = blpapi.SessionOptions()
        self.session_options.setServerHost(host)
        self.session_options.setServerPort(port)
        self.session = None

    def _start_session(self) -> bool:
        try:
            if not self.session:
                self.session = blpapi.Session(self.session_options)
            if not self.session.start():
                print("Failed to start session.")
                return False
            if not self.session.openService("//blp/refdata"):
                print("Failed to open //blp/refdata")
                return False
            return True
        except blpapi.Exception as e:
            print(f"Bloomberg API exception: {e}")
            return False

    def _stop_session(self):
        if self.session:
            self.session.stop()
            self.session = None

    def _prepare_security(self, security: str) -> str:
        return f"/cusip/{security}" if len(security) == 9 else security

    def _process_security_data(self, security_data: blpapi.Element, field: str) -> Dict:
        security = security_data.getElementAsString("security")
        if security_data.hasElement("securityError"):
            error_msg = security_data.getElement("securityError")
            print(f"Security error for {security}: {error_msg}")
            return {}

        field_data = security_data.getElement("fieldData")
        if field_data.hasElement(field):
            price = field_data.getElementAsFloat(field)
            return {security: price}
        else:
            print(f"{field} not found for security: {security}")
            return {}

    def get_latest_prices(self, securities: List[str]) -> pd.DataFrame:
        if not self._start_session():
            return []

        try:
            service = self.session.getService("//blp/refdata")
            request = service.createRequest("ReferenceDataRequest")

            for security in securities:
                request.getElement("securities").appendValue(
                    self._prepare_security(security)
                )
            request.getElement("fields").appendValue("PX_LAST")

            self.session.sendRequest(request, correlationId=blpapi.CorrelationId(1))

            prices = {}
            while True:
                event = self.session.nextEvent(500)  # Wait for 500ms max
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        if msg.messageType() == "ReferenceDataResponse":
                            for security_data in msg.getElement(
                                "securityData"
                            ).values():
                                security = security_data.getElementAsString("security")
                                if security.startswith("/cusip/"):
                                    security = security[7:]  # Remove "/cusip/" prefix
                                if security_data.hasElement("securityError"):
                                    error_msg = security_data.getElement(
                                        "securityError"
                                    )
                                    print(f"Security error for {security}: {error_msg}")
                                else:
                                    field_data = security_data.getElement("fieldData")
                                    if field_data.hasElement("PX_LAST"):
                                        price = field_data.getElementAsFloat("PX_LAST")
                                        prices[benchmark_ticker.get(security)] = price
                                    else:
                                        print(
                                            f"PX_LAST not found for security: {security}"
                                        )
                    break
                elif event.eventType() == blpapi.Event.TIMEOUT:
                    print("Timeout occurred while waiting for response.")
                    break

            benchmark_date = datetime.now().strftime("%Y-%m-%d")
            timestamp = datetime.now()

            data = {
                "benchmark_date": benchmark_date,
                "timestamp": timestamp,
                **prices,
            }

            return pd.DataFrame([data])

        except blpapi.Exception as e:
            print(f"Bloomberg API exception: {e}")
            return []
        finally:
            self._stop_session()

    def get_historical_prices(
        self, securities: List[str], custom_date: str
    ) -> Dict[str, Dict[str, float]]:
        if not self._start_session():
            return {}

        try:
            service = self.session.getService("//blp/refdata")
            prices = {}

            for security in securities:
                request = service.createRequest("HistoricalDataRequest")
                request.getElement("securities").appendValue(
                    self._prepare_security(security)
                )
                request.getElement("fields").appendValue("PX_LAST")
                request.set("startDate", custom_date)
                request.set("endDate", custom_date)

                self.session.sendRequest(request, correlationId=blpapi.CorrelationId(1))

                while True:
                    event = self.session.nextEvent(500)  # Wait for 500ms max
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            if msg.messageType() == "HistoricalDataResponse":
                                security_data = msg.getElement("securityData")
                                if not security_data.hasElement("securityError"):
                                    field_data_array = security_data.getElement(
                                        "fieldData"
                                    )
                                    for field_data in field_data_array.values():
                                        date = field_data.getElementAsString("date")
                                        if field_data.hasElement("PX_LAST"):
                                            price = field_data.getElement(
                                                "PX_LAST"
                                            ).getValueAsFloat()
                                            prices[security] = {
                                                "date": date,
                                                "price": price,
                                            }
                                        else:
                                            print(
                                                f"PX_LAST not found for security: {security}"
                                            )
                                else:
                                    error_msg = security_data.getElement(
                                        "securityError"
                                    )
                                    print(f"Security error for {security}: {error_msg}")
                        break
                    elif event.eventType() == blpapi.Event.TIMEOUT:
                        print(
                            f"Timeout occurred while waiting for response for security: {security}"
                        )
                        break

            return prices
        except blpapi.Exception as e:
            print(f"Bloomberg API exception: {e}")
            return {}
        finally:
            self._stop_session()


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])

                value_placeholders = ", ".join(
                    [
                        f":{col.replace(' ', '_').replace('/', '_')}"
                        for col in df.columns
                    ]
                )
                # NOTE: THIS WORKS! For MS SQL, 'nan' data must be converted to None this way
                df = df.astype(object).where(pd.notnull(df), None)

                if PUBLISH_TO_PROD:
                    # Using MERGE statement for MS SQL Server
                    update_clause = ", ".join(
                        [
                            f'"{col}" = SOURCE."{col}"'
                            for col in df.columns
                            if col != "benchmark_date"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        MERGE INTO {tb_name} AS TARGET
                        USING (SELECT {','.join(f'SOURCE."{col}"' for col in df.columns)} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                        ON TARGET."benchmark_date" = SOURCE."benchmark_date"
                        WHEN MATCHED THEN
                            UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({column_names}) VALUES ({','.join(f'SOURCE."{col}"' for col in df.columns)});
                        """
                    )
                else:
                    update_clause = ", ".join(
                        [
                            f'"{col}"=EXCLUDED."{col}"'
                            for col in df.columns
                            if col
                            != "benchmark_date"  # Assuming "benchmark_date" is unique and used for conflict resolution
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("benchmark_date")
                        DO UPDATE SET {update_clause};
                        """
                    )

                # Replace spaces and slashes with underscores in the DataFrame column names
                df.columns = [
                    col.replace(" ", "_").replace("/", "_") for col in df.columns
                ]

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Latest data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


###### BOND DATA REVAMP #####
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


def get_bond_list():
    """
    Bond list:
    - List of all cusips from Helix
    - Add 38178DAA5
    - Cusips from S:/Lucid/Data/Bond Data/Non-Collateral Cusips.xlsx from both columns:
        "Vantage Proxies"
        "Other"
    - All the values in diff_cusip_map. These are cusips in BBerg but different ticker to access
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
    - Remove Hardwired cusips:
        special_bond_data = fetch_spec_df()
        special_cusips = [x for x in special_bond_data.index]

    - Remove 'PNI' cusips

    - Transform to Bloomberg format:
        cusip_pass = [("/cusip/" if len(x) == 9 else "/mtge/" if x in ('3137F8RH8','3137F8ZC0') else "/isin/") + x for x in cusip_pass]




    """
    # List of additional cusips not included in the Helix query
    non_collateral_cusip_file_path = get_file_path(
        "S:/Lucid/Data/Bond Data/Non-Collateral Cusips.xlsx"
    )
    additional_cusips_df = pd.read_excel(non_collateral_cusip_file_path, skiprows=3)
    additional_cusips_list = additional_cusips_df["Vantage Proxies"].tolist() + [
        "38178DAA5"
    ]

    fields = [
        "SECURITY_TYP,ISSUER,Collat Typ,Name,Industry Sector,Issue DT,Maturity,Amt Outstanding,Coupon,Floater,"
        "MTG Factor,PX Bid,PX Mid,Int Acc,Mtg WAL,MTG ORIG_WAL,DUR ADJ OAS BID,YAS_MOD_DUR,Days Acc,YLD_ytm_BID,I_SPRD_BID,"
        "FLT_SPREAD,OAS_SPREAD_ASK,MTG TRANCHE TYP LONG,MTG PL CPR 1M,MTG PL CPR 6M,MTG_WHLN_GEO1,MTG_WHLN_GEO2,"
        "MTG_WHLN_GEO3,RTG_SP,RTG_MOODY,RTG_FITCH,RTG_KBRA,RTG_DBRS,RTG_EGAN_JONES,DELIVERY_TYP,DTC_REGISTERED,DTC_ELIGIBLE,MTG_DTC_TYP,"
        "TRADE_DT_ACC_INT,PRINCIPAL_FACTOR,MTG_PREV_FACTOR,MTG_RECORD_DT,MTG_FACTOR_PAY_DT,MTG_NXT_PAY_DT_SET_DT,IDX_RATIO"
    ]

    cols = [
        "CUSIP,SECURITY_TYP,ISSUER,Collat Typ,Name,Industry Sector,Issue DT,Maturity,Amt Outstanding,Coupon,Floater,"
        "MTG Factor,PX Bid,PX Mid,Int Acc,Mtg WAL,DUR ADJ OAS BID,YAS_MOD_DUR,USED DURATION,Days Acc,YLD_ytm_BID,"
        "I_SPRD_BID,FLT_SPREAD,OAS_SPREAD_ASK,MTG TRANCHE TYP LONG,MTG PL CPR 1M,MTG PL CPR 6M,MTG_WHLN_GEO1,"
        "MTG_WHLN_GEO2,MTG_WHLN_GEO3,RATINGS BUCKET,RTG_SP,RTG_MOODY,RTG_FITCH,RTG_KBRA,RTG_DBRS,RTG_EGAN_JONES,"
        "DELIVERY_TYP,Est'd Asset Class,CUSIP or ISIN,MTG_PREV_FACTOR,MTG_RECORD_DT,MTG_FACTOR_PAY_DT,MTG_NXT_PAY_DT_SET_DT,IDX_RATIO"
    ]

    records = execute_sql_query(
        daily_price_securities_helix_query, helix_db_type, params=[]
    )
    cusips_list = records["BondID"].tolist()

    # Excluding all PNI cusips
    cusips_list = [
        cusip for cusip in cusips_list if not (len(cusip) >= 3 and cusip[:3] == "PNI")
    ]

    joined_cusips_list = list(set(cusips_list) | set(additional_cusips_list))

    print(joined_cusips_list)
    return cusips_list


# Example usage:
if __name__ == "__main__":
    securities = [
        "TSFR1M Index",
        "TSFR3M Index",
        "TSFR6M Index",
        "TSFR12M Index",
        "US0001M Index",
        "US0003M Index",
        "DCPA030Y Index",
        "DCPA090Y Index",
        "DCPA180Y Index",
        "DCPA270Y Index",
    ]
    custom_date = "20240618"  # Specify the desired date in YYYYMMDD format

    # # Assuming get_database_engine is already defined and returns a SQLAlchemy engine
    if PUBLISH_TO_PROD:
        engine = get_database_engine("sql_server_2")
    else:
        engine = get_database_engine("postgres")
    #
    # fetcher = BloombergDataFetcher()
    #
    # print("Fetching latest prices...")
    # prices_latest_df = fetcher.get_latest_prices(securities)
    # print(prices_latest_df)
    #
    # print("Upserting data to table...")
    # upsert_data(tb_name, prices_latest_df)
    sec_list = get_bond_list()
    print(sec_list)
