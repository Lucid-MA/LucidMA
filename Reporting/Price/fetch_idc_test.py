import http.client
import urllib.parse
from base64 import b64encode

# Basic authentication header with updated credentials
userAndPass = b64encode(b"d4lucid:Spring17!").decode("ascii")
headers = {'Authorization': 'Basic %s' % userAndPass}

# Parameters for the request
params = urllib.parse.urlencode({'Request': 'GET,("58550NBQ3","JPM-MANT1","89640NAA6","03769UAE8","126686AH7"),PRC,20240422,20240424,D,TITLES=SHORT,DATEFORM=YMD', 'Done': 'flag'})
# Create an HTTPS connection
conn = http.client.HTTPSConnection("rplus.icedataservices.com")

# Make a POST request
conn.request("POST", "/cgi/nph-rplus", params, headers)

# Get the response from the server
response = conn.getresponse()

# Print the response content
print(response.read().decode())

# Close the connection
conn.close()