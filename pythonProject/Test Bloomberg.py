import blpapi

def query_bloomberg_cusip(cusip):
    # Create a Session
    sessionOptions = blpapi.SessionOptions()
    session = blpapi.Session(sessionOptions)

    # Start the session
    if not session.start():
        print("Failed to start session.")
        return

    # Open a service
    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    # Create and send Request
    service = session.getService("//blp/refdata")
    request = service.createRequest("ReferenceDataRequest")

    # Add securities to request
    request.append("securities", cusip)

    # Add fields to request
    request.append("fields", "PX_LAST")

    # Send request
    session.sendRequest(request)

    # Process response
    while(True):
        # We provide timeout to give an opportunity for Ctrl+C to interrupt
        event = session.nextEvent(500)
        if event.eventType() == blpapi.Event.RESPONSE:
            for msg in event:
                securities = msg.getElement("securityData")
                for security in securities.values():
                    fieldData = security.getElement("fieldData")
                    lastPrice = fieldData.getElementAsFloat("PX_LAST")
                    print(f"Last Price for {cusip}: {lastPrice}")
            break

    # Stop the session
    session.stop()

if __name__ == "__main__":
    cusip = "123456"
    query_bloomberg_cusip(cusip)