import blpapi

def get_bloomberg_prices(cusips):
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

    for cusip in cusips:
        request.getElement("securities").appendValue(cusip)

    request.getElement("fields").appendValue("PX_LAST")

    cid = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=cid)

    prices = {}  # Dictionary to store prices, with CUSIPs as keys

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

# Example usage:
cusips = ["USOSFR10 Index", "DCPA090Y Index", "TSFR3M Index", "912828GM6"]
prices = get_bloomberg_prices(cusips)
print(prices)