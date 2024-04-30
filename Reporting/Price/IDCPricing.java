/**
	adapted from given remoteplus java api. purely scripting.
	fetch the most recent prices from IDC for given cusips
	writes to given filename
	August, 2019
*/


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import java.util.HashMap;
import java.util.Base64;

public class IDCPricing {

	private static HashMap<String, Double> idcPrices = new HashMap<String, Double>();


	/**
		Populates idcPrices with prices from IDC. Sends HTTPS request using remoteplus
		Redundant to populate a map from a csv string and then re-enter into csv string, but allows formatting/debugging flexibility (and was a step in debugging, just decided to keep it)
	*/
	private static void fetchIDC(String command) {
		String urlString = "https://rplus.intdata.com/cgi/nph-rplus";
		String user = "d4lucid";
		String password = "Spring17!";

		try {
			URL url = new URL(urlString);
	        // Get a URLConnection object, to write to POST method
	        URLConnection connect = url.openConnection();
	    
	        // Specify connection settings
	        connect.setDoInput(true);
	        connect.setDoOutput(true);
	        String up = user + ":" + password;

// 	        String userpass = javax.xml.bind.DatatypeConverter.printBase64Binary(up.getBytes());
	        String userpass = Base64.getEncoder().encodeToString(up.getBytes());
	        connect.setRequestProperty("Authorization","Basic " + userpass);
	    
	        // Create a print stream, for easy writing
	        PrintStream out = new PrintStream (connect.getOutputStream());
	    
	        StringBuffer outbuf = new StringBuffer();
	        outbuf.append(URLEncoder.encode("Request","UTF-8"));
	        outbuf.append("=");
	        outbuf.append(URLEncoder.encode(command,"UTF-8"));
	        outbuf.append("&Done=flag");
	                
	        out.print(outbuf);
	        out.close();
	        // Get the result stream
	        InputStream input = connect.getInputStream();
	                
	        byte[] inbuf = new byte[1024];
	        int count;
	        StringBuilder rplusData = new StringBuilder();
	        while ((count = input.read(inbuf)) >= 0) {
	            rplusData.append(new String(inbuf, 0, count));
	        }

	        String[] lines = rplusData.toString().split("\\r?\\n");
	        for (int i = 1; i < lines.length - 1; i++) {
	        	String[] entry = lines[i].split(",");
	        	try {
	        		idcPrices.put(entry[0].replace("\"", ""), Double.parseDouble(entry[1]));
	        	} catch (NumberFormatException e) {
	        		continue;
	        	}
	        }

		} catch (MalformedURLException mue) {
                System.err.println ("Bad URL - " + urlString);
        } catch (IOException ioe) {
                System.err.println ("I/O error " + ioe);
        }
	}

	public static void main(String[] args) {
		String urlString = "https://rplus.intdata.com/cgi/nph-rplus";
		String user = "d4lucid";
		String password = "Spring17!";

		fetchIDC(args[0]);
		for (String cusip : idcPrices.keySet()) {
			System.out.print(cusip + "," + idcPrices.get(cusip) + ";");
		}
	}
}