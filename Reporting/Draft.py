import blpapi
from blpapi import SessionOptions, Session, Event


def print_header():
    print(f"{'Field ID':<20} {'Field Name':<40} {'Field Type':<20}")
    print("=" * 80)


def print_field(field_element):
    field_id = field_element.getElementAsString("id")
    field_name = field_element.getElementAsString("mnemonic") if field_element.hasElement("mnemonic") else "N/A"
    field_type = field_element.getElementAsString("datatype") if field_element.hasElement("datatype") else "N/A"

    print(f"{field_id:<20} {field_name:<40} {field_type:<20}")


def main():
    session_options = SessionOptions()
    session_options.setServerHost("localhost")
    session_options.setServerPort(8194)

    session = Session(session_options)

    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/apiflds"):
        print("Failed to open //blp/apiflds service.")
        return

    field_info_service = session.getService("//blp/apiflds")
    request = field_info_service.createRequest("FieldListRequest")
    request.set("fieldType", "Static")
    request.set("returnFieldDocumentation", True)

    print("Sending Request:", request)
    session.sendRequest(request)

    try:
        while True:
            event = session.nextEvent()

            if event.eventType() in [Event.PARTIAL_RESPONSE, Event.RESPONSE]:
                for msg in event:
                    if msg.hasElement("fieldData"):
                        fields = msg.getElement("fieldData")
                        num_fields = fields.numValues()

                        print_header()
                        for i in range(num_fields):
                            print_field(fields.getValueAsElement(i))
                        print()

                if event.eventType() == Event.RESPONSE:
                    break
    finally:
        session.stop()


if __name__ == "__main__":
    main()