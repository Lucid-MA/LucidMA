import subprocess
from datetime import datetime
from pathlib import PureWindowsPath

import os
from datetime import datetime
# from utils import (
#     form_as_percent, bps_spread, benchmark_shorten, month_wordify,
#     wordify, secured_by_from, exp_rat_footnote, addl_coll_breakdown,
#     colltable, performance_graph, return_table_plot
# )

def generate_fund_report(ws, overview_ws, config):
    report_name = ws.title
    crow = 7
    while ws["F" + str(crow)].value:
        if ws["F" + str(crow)].value == ws["C23"].value:
            break
        crow += 1

    prev_pd_start = ws["E" + str(crow)].value
    this_pd_start = ws["F" + str(crow)].value

    overview_row = 7
    while overview_ws["B" + str(overview_row)].value:
        if overview_ws["B" + str(overview_row)].value == this_pd_start:
            break
        overview_row += 1

    if not overview_ws["B" + str(overview_row)].value:
        print(f"ERROR: Overview row not found for this period in {report_name}. Skipping...")
        return None

    report_date = datetime.now()
    lucid_aum = overview_ws["H" + str(overview_row)].value
    program_size = 0
    for col in "CDEFG":
        if overview_ws[col + "6"].value:
            if overview_ws[col + "6"].value.upper() == ws["C9"].value.upper():
                program_size = overview_ws[col + str(overview_row)].value
                break

    # Extract other required data points and calculations here

    fund_description = config["fund_descriptions"].get(ws["C9"].value.upper(), "")
    series_description = config["series_descriptions"].get(ws["C6"].value.upper(), "")

     script = config["fund_report_template"].format(
        report_date=report_date.strftime("%B %d, %Y"),
        fundname=ws["C9"].value,
        series_abbrev=ws["C11"].value,
        fund_description=fund_description,
        series_description=series_description,
        # Add other required parameters here
    )

    return generate_pdf(script, report_name)

def generate_note_report(ws, overview_ws, config):
    report_name = ws.title
    crow = 7
    while ws["F" + str(crow)].value:
        if ws["F" + str(crow)].value == ws["C23"].value:
            break
        crow += 1

    prev_pd_start = ws["E" + str(crow)].value
    this_pd_start = ws["F" + str(crow)].value

    overview_row = 7
    while overview_ws["B" + str(overview_row)].value:
        if overview_ws["B" + str(overview_row)].value == this_pd_start:
            break
        overview_row += 1

    if not overview_ws["B" + str(overview_row)].value:
        print(f"ERROR: Overview row not found for this period in {report_name}. Skipping...")
        return None

    report_date = datetime.now()
    lucid_aum = overview_ws["H" + str(overview_row)].value
    program_size = 0
    for col in "CDEFG":
        if overview_ws[col + "6"].value:
            if overview_ws[col + "6"].value.upper() == ws["C9"].value.upper():
                program_size = overview_ws[col + str(overview_row)].value
                break

    # Extract other required data points and calculations here

    script = config["note_report_template"].format(
        report_date=report_date.strftime("%B %d, %Y"),
        fundname=ws["C9"].value,
        series_abbrev=ws["C11"].value,
        # Add other required parameters here
    )

    return generate_pdf(script, report_name)

def generate_pdf(script, report_name):
    print("Generating Latex file...")
    filepath = report_name.replace(" ", "_")
    script_file = filepath + ".tex"
    with open(script_file, "w") as out:
        out.write(script)
        out.close()

    print("Generating PDF...")
    pdf_file = filepath + ".pdf"
    cmd_str = f"pdflatex -interaction nonstopmode {script_file} {pdf_file}"
    try:
        subprocess.check_output(cmd_str)
    except subprocess.CalledProcessError as e:
        print(f"Error generating file {pdf_file}: {e}")
        return None
    else:
        print(f"File generated to {pdf_file}")
        return pdf_file