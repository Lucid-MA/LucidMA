import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


/**
	JJ Vulopas, September 2019
	Uses BB Server API to fetch data given security identifiers and fields.
	requires reference to api jar at compile time and run time, "C:\blp\BloombergWindowsSDK\JavaAPI\v3.12.1.1\lib\blpapi3.jar;"
	be sure to include the semicolon in the classpath

	e.g. 
	javac -cp "C:\blp\BloombergWindowsSDK\JavaAPI\v3.12.1.1\lib\blpapi3.jar;" --release 8 BloombergFetch.java
	java -cp "C:\blp\BloombergWindowsSDK\JavaAPI\v3.12.1.1\lib\blpapi3.jar;" BloombergFetch "curr_fetch_batch.txt" "LAST_PRICE, BID, ASK"
	java -cp "C:\blp\BloombergWindowsSDK\JavaAPI\v3.12.1.1\lib\blpapi3.jar;" BloombergFetch "curr_fetch_batch.txt" "YLD_YTM_BID" PX_BID 104 PRICING_SOURCE BVAL
	adapted from given examples in BB Server API SDK

	special failure tag for the processor is xxfailurexx
*/
public class BloombergFetch {

	private static final Name SECURITY_DATA = new Name("securityData");
    private static final Name SECURITY = new Name("security");
    private static final Name FIELD_DATA = new Name("fieldData");
    private static final Name RESPONSE_ERROR = new Name("responseError");
    private static final Name SECURITY_ERROR = new Name("securityError");
    private static final Name FIELD_EXCEPTIONS = new Name("fieldExceptions");
    private static final Name FIELD_ID = new Name("fieldId");
    private static final Name ERROR_INFO = new Name("errorInfo");
    private static final Name CATEGORY = new Name("category");
    private static final Name MESSAGE = new Name("message");

    private TreeMap<String, String> data;

    public BloombergFetch() {
    	data = new TreeMap<String, String>();
    }

	public void fetch(String[] cusips, String[] fields, TreeMap<String, String> params) throws Exception {
		
		// Setup Session parameters: server, port, buffer size, etc...
		SessionOptions opt = new SessionOptions();
		opt.setServerHost("127.0.0.1");
		opt.setServerPort(8194);
		Session session = new Session(opt); // Establish session
		// Start Session
		try {
			if(!session.start()) {
				System.err.println("xxfailurexx");
				System.exit(1);
			}
			if(!session.openService("//blp/refdata")){
				System.err.println("xxfailurexx");
			}

			Service ref = session.getService("//blp/refdata");
			Request req = ref.createRequest("ReferenceDataRequest"); // initiate new request
			
			Element secs = req.getElement("securities");
			Element flds = req.getElement("fields");
			
			for (int i = 0; i < cusips.length; i++) {
				secs.appendValue(cusips[i]); // moved functionality to handle cusips to caller
				// if (cusips[i].length() == 9) {
				//   	secs.appendValue("/cusip/" + cusips[i]);
				// } else {
				//  	secs.appendValue("/isin/" + cusips[i]);
				// }
				// secs.appendValue(cusips[i]); // just for testing
			}

			for (int i = 0; i < fields.length; i++) {
				flds.appendValue(fields[i]);
			}

			Element overrides = req.getElement("overrides");

			if (!params.isEmpty()) {
				for (String paramFld : params.keySet()) {
					Element addl = overrides.appendElement();
					addl.setElement("fieldId", paramFld);
					addl.setElement("value", params.get(paramFld));
				}
			}

			// req.set("startDate", "20190912");
			// req.set("endDate", "20190912");

			// send request
			session.sendRequest(req, null);

			String outp = "";

			// process responses
			boolean done = false;
	        while (!done) {
	            Event event = session.nextEvent();
	            if (event.eventType() == Event.EventType.PARTIAL_RESPONSE) {
	               //System.out.println("Processing Partial Response");
	                handleResponse(event);
	            }
	            else if (event.eventType() == Event.EventType.RESPONSE) {
	                //System.out.println("Processing Response");
	                handleResponse(event);
	                done = true;
	            } else {
	                MessageIterator msgIter = event.messageIterator();
	                while (msgIter.hasNext()) {
	                    Message msg = msgIter.next();
	                    //System.out.println(msg.asElement());
	                    if (event.eventType() == Event.EventType.SESSION_STATUS) {
	                        if (msg.messageType().equals("SessionTerminated") ||
	                            msg.messageType().equals("SessionStartupFailure")) {
	                            done = true;
	                        }
	                    }
	                }
	            }
	        }
	    } catch (Exception e) {
        	session.stop();
        	System.out.println("xxfailurexx");
        	e.printStackTrace();
        	System.exit(1);
    	}
    	session.stop();
	}

	// so if na just doesn't include, but if bad field (not applicable) includes exception
	private void handleResponse(Event event) throws Exception {
		MessageIterator msgIter = event.messageIterator();
		while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.hasElement(RESPONSE_ERROR)) {
            	System.out.println("xxfailurexx");
                printErrorInfo("Failed request: ", msg.getElement(RESPONSE_ERROR));
                continue;
            }

            Element securities = msg.getElement(SECURITY_DATA);
            int numSecurities = securities.numValues();
            //System.out.println("Processing " + numSecurities + " securities:");
            for (int i = 0; i < numSecurities; i++) {
                Element security = securities.getValueAsElement(i);
                String taggedCusip = security.getElementAsString(SECURITY);
                //System.out.println("\nSecurity: " + taggedCusip);

                if (security.hasElement("securityError")) {
                    printErrorInfo("\tSECURITY FAILED: ",
                                   security.getElement(SECURITY_ERROR));
                    // System.out.println("xxfailurexx"); not here
                    continue;
                }

                String respToAdd = ""; // value to add to map
                if (security.hasElement(FIELD_DATA)) {
                    Element fields = security.getElement(FIELD_DATA);
                    if (fields.numElements() > 0) {
                        //System.out.println("FIELD\t\tVALUE");
                        //System.out.println("-----\t\t-----");
                        int numElements = fields.numElements();
                        for (int j = 0; j < numElements; ++j) {
                            Element field = fields.getElement(j);
                            //System.out.println(field.name() + "\t\t" +
                            //                   field.getValueAsString());
                            respToAdd += field.name() + "~" + field.getValueAsString() + ";"; // looks like there's a leading space
                        }
                    }
                }
                data.put(taggedCusip, respToAdd); // add the data to map
                System.out.println("");
                Element fieldExceptions = security.getElement(FIELD_EXCEPTIONS);
                if (fieldExceptions.numValues() > 0) {
                   // System.out.println("FIELD\t\tEXCEPTION");
                    //System.out.println("-----\t\t---------");
                    for (int k = 0; k < fieldExceptions.numValues(); ++k) {
                        Element fieldException =
                            fieldExceptions.getValueAsElement(k);
                        //printErrorInfo(fieldException.getElementAsString(FIELD_ID) +
                        //        "\t\t", fieldException.getElement(ERROR_INFO));
                    }
                }
            }
        }
	}


	private static String rawSec(String tagged) {
		if (tagged.startsWith("/cusip/")) {
			return tagged.substring(7, tagged.length());
		}
		if (tagged.startsWith("/isin/")) {
			return tagged.substring(6, tagged.length());
		}
		return tagged;
	}

	private static void printErrorInfo(String leadingStr, Element errorInfo) throws Exception {
        System.out.println(leadingStr + errorInfo.getElementAsString(CATEGORY) +
                           " (" + errorInfo.getElementAsString(MESSAGE) + ")");
    }
	public static void main(String[] args) throws Exception {
		BloombergFetch fetcher = new BloombergFetch();

		// contains info of whether ISIN or SEDOL
		// Sept 2020 (first edit), changed to read from a file instead of cmd line so no overload
		BufferedReader fIn;
		String cusipStr = "";
		try {
			fIn = new BufferedReader(new FileReader(args[0]));
			cusipStr = fIn.readLine();
			fIn.close();
		} catch (IOException e) {
			System.err.println("xxfailurexx");
		}

		TreeMap<String, String> params = new TreeMap<String, String>();
		int i = 2;
		boolean done = false;
		// will only populate if override args there on cmd line
		while(!done) {
			try {
				params.put(args[i], args[i + 1]);
				i += 2;
			} catch (Exception e) {
				done = true;
			}
		}

		fetcher.fetch(cusipStr.split(","), args[1].split(","), params); 

		//System.out.println("Now for the output...");
		for (String cusip: fetcher.data.keySet()) {

			System.out.print(rawSec(cusip) + ":" );
			System.out.println(fetcher.data.get(cusip));
		}
	}

	/**
		Simple wrapper for a table. Could easily generalize beyond just cusips and fields, but I don't
		imagine using Java for this again, so making it specific to this application.
	*/
	// private class FieldTable {
	// 	private String[] cols;
	// 	private String[][] data;
		
	// 	FieldTable(String[] fields, String[] cusips) {
	// 		cols = fields;
	// 		data = new String[cusips.length][cols.length];
	// 		for (int i = 0; i < cusips.length; i++) {
	// 			data[i][0] = cusips[i]; 
	// 		}
	// 	}

	// 	/**
	// 		Add new elt to table, overriding and returning if one already existed
	// 	*/
	// 	public String set(String cusip, String field, String value) {
	// 		int col = 0;
	// 		int row = 0;

	// 		for (int i = 0; i < cols.length; i++) {
	// 			if (cols[i].equals(field)) {
	// 				col = i;
	// 				break;
	// 			}
	// 		}

	// 		for (int i = 0; i < data[0].length; i++) {
	// 			if (data[i])
	// 		}
	// 	}
	// }
}