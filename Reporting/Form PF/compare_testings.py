import difflib


GOOD_PATH = "lucid_form_pf_20211013_10_35_04.xml"
BAD_PATH = "ignore.xml"

print("Opening files.")
good = open(GOOD_PATH, 'r').read()
print("Fetched good.")
bad = open(BAD_PATH, 'r').read()
print("Fetched bad.")

good_length = len(good)
bad_length = len(bad)

print("Comparing good to bad.")
print("Good length: " + str(good_length))
print("Bad length: " + str(bad_length))

for i in range(min(good_length,bad_length)):
	try:
		if good[i] != bad[i]:
			print(str(i) + ": ")
			print("Good: " + str(good[i]))
			print("Bad: " + str(bad[i]))
			break	
	except:
		break