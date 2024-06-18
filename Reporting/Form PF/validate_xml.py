from lxml import etree

XML_OUTPUT_PATH = "2024/01.15.24/lucid_form_pf_20240425_12_53_34.xml"

XSD_PATH = "PFFormFiling.xsd"

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
