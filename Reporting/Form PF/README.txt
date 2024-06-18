1) move the contents of the curr/ directory into the directory for the previous quarter within prev.  the curr/ directory will store the working files for this report

2) create an empty "q63" directory in the curr directory

3) Form PF Q63 XML Generator.xlsx - put in dates and all funds and series for q63. This will populate the filepaths for the NAV calculators for the three month-ends of the quarter (if month end was on a weekend may need tochange the filepath to include "BOD"). Click generate. This generates the position statements necessary for Q63 part e into curr/q63. Any files that don't exist will be red. 

4) copy the Form PF workbook (martin populates) into the curr/ directory and rename it "q63_book.xlsx". For each series, populate the orange cells of the "Sec 3 Item A-C" tab (55d-i) and the "Sec 3 Item D-E" tab (62). These figures are reported on the "info" tabs of each of the q63 workbooks generated in the above step (can copy range info!O1:T3 and paste-transpose-values)

5) generate_form_pf_xml.py - populate all parameters (all caps variables in top of value). This will generate the xml output into the curr directory and validate it against the most up-to-date schema provided by the SEC. if the validation fails it will throw a warning

(

	if the python script fails with something that looks like this:

		{Could not add module (IID('{00020813-0000-0000-C000-000000000046}'), 0, 1, 9) - <class 'AttributeError'>: module 'win32com.gen_py.00020813-0000-0000-C000-000000000046x0x1x9' has no attribute 'CLSIDToClassMap'}

	then need to delete the directory within the gen_py directory on your system that's called "0002...." to refresh. can find it with:

	>>> import win32com
	>>> print(win32com.__gen_path__)

for example, delete: "C:\Users\jvulopas\AppData\Local\Temp\gen_py\3.7\00020813-0000-0000-C000-000000000046x0x1x9"
)


(

if getting this error:

	pywintypes.com_error: (-2147352567, 'Exception occurred.', (0, None, None, None, 0, -2147352565), None)

it's most likely because one of the sheetnames is wrong (ie it used to be "B and C" but now it's "B & C")

)

6) if needed, can run validate_xml.py to ensure that the submission matches the schema