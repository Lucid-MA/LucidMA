import blpapi


def get_bloomberg_prices(cusips):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)
    session = blpapi.Session(sessionOptions)

    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/mktdata"):
        print("Failed to open //blp/mktdata")
        return

    service = session.getService("//blp/mktdata")
    request = service.createRequest("ReferenceDataRequest")

    for cusip in cusips:
        request.getElement("securities").appendValue(cusip)

    request.getElement("fields").appendValue("PX_LAST")

    cid = blpapi.CorrelationId(1)
    session.sendRequest(request, correlationId=cid)

    while True:
        ev = session.nextEvent()
        for msg in ev:
            if (
                ev.eventType() == blpapi.Event.RESPONSE
                or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE
            ):
                for securityData in msg.getElement("securityData").values():
                    security = securityData.getElementAsString("security")
                    fieldData = securityData.getElement("fieldData")
                    price = fieldData.getElementAsFloat("PX_LAST")
                    print(f"Security: {security}, Price: {price}")
            if ev.eventType() == blpapi.Event.RESPONSE:
                break


# Example usage:
cusips = ["CUSIP1", "CUSIP2", "CUSIP3"]
get_bloomberg_prices(cusips)
