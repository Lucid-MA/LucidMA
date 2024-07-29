import blpapi

def get_bloomberg_latest_prices(securities):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    session = blpapi.Session(sessionOptions)

    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    service = session.getService("//blp/refdata")
    request = service.createRequest("ReferenceDataRequest")

    for security in securities:
        if len(security) == 9:
            security = "/cusip/" + security
        request.getElement("securities").appendValue(security)

    request.getElement("fields").appendValue("PX_LAST")

    cid = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=cid)

    prices = {}  # Dictionary to store prices, with securities as keys

    while True:
        ev = session.nextEvent()
        if ev.eventType() == blpapi.Event.RESPONSE:
            for msg in ev:
                if msg.messageType() == "ReferenceDataResponse":
                    for securityData in msg.getElement("securityData").values():
                        security = securityData.getElementAsString("security")

                        if securityData.hasElement("securityError"):
                            errorMsg = securityData.getElement("securityError")
                            print(f"Security error for {security}: {errorMsg}")
                        else:
                            fieldData = securityData.getElement("fieldData")
                            if fieldData.hasElement("PX_LAST"):
                                price = fieldData.getElementAsFloat("PX_LAST")
                                prices[security] = price  # Store price in the dictionary
                            else:
                                print(f"PX_LAST not found for security: {security}")
            break  # Break the loop after processing the RESPONSE event

    session.stop()  # Stop the Bloomberg session
    return prices  # Return the dictionary of prices

def get_bloomberg_historical_prices(securities, custom_date):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    session = blpapi.Session(sessionOptions)

    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    service = session.getService("//blp/refdata")

    prices = {}  # Dictionary to store prices, with securities as keys

    for security in securities:
        request = service.createRequest("HistoricalDataRequest")

        if len(security) == 9:
            security_with_prefix = "/cusip/" + security
            request.getElement("securities").appendValue(security_with_prefix)
        else:
            request.getElement("securities").appendValue(security)

        request.getElement("fields").appendValue("PX_LAST")

        # Set the custom date for which you want to retrieve the price
        request.set("startDate", custom_date)
        request.set("endDate", custom_date)

        cid = blpapi.CorrelationId(1)
        session.sendRequest(request, correlationId=cid)

        while True:
            ev = session.nextEvent()
            if ev.eventType() == blpapi.Event.RESPONSE:
                for msg in ev:
                    if msg.messageType() == "HistoricalDataResponse":
                        securityData = msg.getElement("securityData")

                        if securityData.hasElement("securityError"):
                            errorMsg = securityData.getElement("securityError")
                            print(f"Security error for {security}: {errorMsg}")
                        else:
                            fieldDataArray = securityData.getElement("fieldData")
                            for fieldData in fieldDataArray.values():
                                date = fieldData.getElementAsString("date")
                                if fieldData.hasElement("PX_LAST"):
                                    price = fieldData.getElement("PX_LAST").getValueAsFloat()
                                    prices[security] = {"date": date, "price": price}  # Store date and price in the dictionary
                                else:
                                    print(f"PX_LAST not found for security: {security}")
                break  # Break the loop after processing the RESPONSE event

    session.stop()  # Stop the Bloomberg session
    return prices  # Return the dictionary of prices

# Example usage:
securities = ["TSFR1M Index", "TSFR3M Index", "TSFR6M Index", "TSFR12M Index", "38383PU74"]
custom_date = "20240618"  # Specify the desired date in YYYYMMDD format
prices_historical = get_bloomberg_historical_prices(securities, custom_date)
prices_latest = get_bloomberg_latest_prices(securities)
print(prices_historical)
print(prices_latest)