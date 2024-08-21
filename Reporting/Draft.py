import blpapi
from blpapi import SessionOptions, Session


def print_header():
    print(f"{'Field ID':<20} {'Field Name':<40} {'Field Type':<20}")


def print_field(field_element):
    field_id = field_element.getElementAsString("id")
    field_name = field_element.getElementAsString("mnemonic")
    field_type = field_element.getElementAsString("datatype")
    print(f"{field_id:<20} {field_name:<40} {field_type:<20}")


def main():
    # Session options and session creation
    session_options = SessionOptions()
    session_options.setServerHost("localhost")
    session_options.setServerPort(8194)

    session = Session(session_options)

    # Start the session
    if not session.start():
        print("Failed to start session.")
        return

    # Open the field information service
    if not session.openService("//blp/apiflds"):
        print("Failed to open //blp/apiflds service.")
        return

    # Create the field list request
    field_info_service = session.getService("//blp/apiflds")
    request = field_info_service.createRequest("FieldListRequest")
    request.set("fieldType", "All")
    request.set("returnFieldDocumentation", True)

    print("Sending Request:", request)
    session.sendRequest(request)

    # Event loop to process response
    while True:
        event = session.nextEvent()
        for msg in event:
            if msg.hasElement("fieldData"):
                fields = msg.getElement("fieldData")
                num_fields = fields.numValues()
                print_header()
                for i in range(num_fields):
                    print_field(fields.getValueAsElement(i))

        # Exit loop after receiving the final response
        if event.eventType() == blpapi.Event.RESPONSE:
            break


if __name__ == "__main__":
    main()
