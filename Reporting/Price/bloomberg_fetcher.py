import blpapi
from typing import List, Dict

class BloombergDataFetcher:
    def __init__(self, host: str = 'localhost', port: int = 8194):
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

    def get_latest_prices(self, securities: List[str]) -> Dict[str, Dict[str, float]]:
        if not self._start_session():
            return {}

        try:
            service = self.session.getService("//blp/refdata")
            request = service.createRequest("ReferenceDataRequest")

            for security in securities:
                request.getElement("securities").appendValue(self._prepare_security(security))
            request.getElement("fields").appendValue("PX_LAST")

            self.session.sendRequest(request, correlationId=blpapi.CorrelationId(1))

            prices = {}
            while True:
                event = self.session.nextEvent(500)  # Wait for 500ms max
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        if msg.messageType() == "ReferenceDataResponse":
                            for security_data in msg.getElement("securityData").values():
                                security = security_data.getElementAsString("security")
                                if security.startswith("/cusip/"):
                                    security = security[7:]  # Remove "/cusip/" prefix
                                if security_data.hasElement("securityError"):
                                    error_msg = security_data.getElement("securityError")
                                    print(f"Security error for {security}: {error_msg}")
                                else:
                                    field_data = security_data.getElement("fieldData")
                                    if field_data.hasElement("PX_LAST"):
                                        price = field_data.getElementAsFloat("PX_LAST")
                                        prices[security] = {"date": "latest", "price": price}
                                    else:
                                        print(f"PX_LAST not found for security: {security}")
                    break
                elif event.eventType() == blpapi.Event.TIMEOUT:
                    print("Timeout occurred while waiting for response.")
                    break

            return prices
        except blpapi.Exception as e:
            print(f"Bloomberg API exception: {e}")
            return {}
        finally:
            self._stop_session()

    def get_historical_prices(self, securities: List[str], custom_date: str) -> Dict[str, Dict[str, float]]:
        if not self._start_session():
            return {}

        try:
            service = self.session.getService("//blp/refdata")
            prices = {}

            for security in securities:
                request = service.createRequest("HistoricalDataRequest")
                request.getElement("securities").appendValue(self._prepare_security(security))
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
                                    field_data_array = security_data.getElement("fieldData")
                                    for field_data in field_data_array.values():
                                        date = field_data.getElementAsString("date")
                                        if field_data.hasElement("PX_LAST"):
                                            price = field_data.getElement("PX_LAST").getValueAsFloat()
                                            prices[security] = {"date": date, "price": price}
                                        else:
                                            print(f"PX_LAST not found for security: {security}")
                                else:
                                    error_msg = security_data.getElement("securityError")
                                    print(f"Security error for {security}: {error_msg}")
                        break
                    elif event.eventType() == blpapi.Event.TIMEOUT:
                        print(f"Timeout occurred while waiting for response for security: {security}")
                        break

            return prices
        except blpapi.Exception as e:
            print(f"Bloomberg API exception: {e}")
            return {}
        finally:
            self._stop_session()

# Example usage remains the same


# Example usage:
if __name__ == "__main__":
    securities = ["TSFR1M Index", "TSFR3M Index", "TSFR6M Index", "TSFR12M Index", "38383PU74"]
    custom_date = "20240618"  # Specify the desired date in YYYYMMDD format

    fetcher = BloombergDataFetcher()

    print("Fetching historical prices...")
    prices_historical = fetcher.get_historical_prices(securities, custom_date)
    print(prices_historical)

    print("\nFetching latest prices...")
    prices_latest = fetcher.get_latest_prices(securities)
    print(prices_latest)