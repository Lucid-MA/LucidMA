"""

Originally created for the Q4-2020 Form PF Filing by Karan Rai

differences between full_output.xml and the submitted form
change DCGXX CUSIP to 262006208 (not X9USDDGCM)
change FILING_TYPE to PF-AMEND (if making multiple versions)
find and replace Other (Government 2a7 Funds) -> Government 2a7 Fund
Change Q53 USG to false

To add a new series, add all of the relevant sheet names to the global constants (SECTION_1B_SHEETS, SECTION_3_SHEETS1, SECTION_3_SHEETS2).
You must also add the Q63 data for the new series to Q63_PATHS
Lastly, you must add the new fund data to FUND_DATA.

ALL OTHER PARAMETERS TO INITIALIZE UP TOP (INCLUDING init_filing() call near top of main)

"""

import xml.etree.ElementTree as ET
import win32com.client as win32
from lxml import etree

# PARAMETERS TO INITIALIZE
#XML_OUTPUT_PATH = "S:\\Mandates\\Funds\\Fund Reporting\\Form PF working files\\lucid_form_pf_20211013_10_35_04.xml"
XML_OUTPUT_PATH = "S:\\Mandates\\Funds\\Fund Reporting\\Form PF working files\\ignore.xml"
XSD_PATH = "S:\\Mandates\\Funds\\Fund Reporting\\Form PF working files\\PFFormFiling.xsd" # one of the JJV additions - used to validate the xml against the xsd schema 

print("Validating XML output...")
try:
    xsd_doc = etree.parse(XSD_PATH)
    xml_schema = etree.XMLSchema(xsd_doc)
    xml_doc = etree.parse(XML_OUTPUT_PATH)
    xml_schema.assertValid(xml_doc)
    print("SUCCESS: File valid.")
except Exception as e:
    print("FAILED: File not valid.")
    print(e)