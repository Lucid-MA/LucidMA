import yaml
import subprocess
from datetime import datetime

def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def populate_template(template, data):
    return template.format(**data)

def generate_pdf(latex_content, output_filename):
    with open(f"{output_filename}.tex", 'w') as file:
        file.write(latex_content)
    subprocess.run(["pdflatex", "-interaction=nonstopmode", f"{output_filename}.tex"], check=True)

def main():
    # Load configuration
    config = load_config("config.yaml")
    fund_report_template = config['fund_report_template']

    # Example data - this would be dynamically loaded in a real scenario
    data = {
        'fundname': 'Prime Fund M',
        'report_date': datetime.now().strftime("%Y-%m-%d"),
        'fund_description': "This fund focuses on short-term securities.",
        'series_description': "The Series M targets stable returns with minimal risk.",
        'series_abbrev': "M",
        'benchmark': "Libor",
        'tgt_outperform': "50",
        'prev_pd_start': "2023-01-01",
        'this_pd_start': "2023-02-01",
        'prev_pd_return': "0.5%",
        'prev_pd_benchmark': "0.3%",
        'prev_pd_outperform': "20 bps",
        'this_pd_end': "2023-03-01",
        'this_pd_est_return': "0.6%",
        'benchmark_short': "Libor",
        'this_pd_est_outperform': "30 bps",
        'tablevstretch': "1.2",
        'return_table_plot': "Data for returns and plots goes here",
        'fund_size': "200M USD",
        'series_size': "50M USD",
        'lucid_aum': "1B USD",
        'rating': "AAA",
        'rating_org': "Moody's",
        'calc_frequency': "Monthly",
        'next_withdrawal_date': "2023-04-01",
        'next_notice_date': "2023-03-15",
        'min_invest': "10,000 USD",
        'wal': "30",
        'legal_fundname': "Prime Fund M LP",
        'fund_inception': "2020-01-01",
        'series_inception': "2020-02-01",
        'performance_graph': "Graph data or image path",
        'toptableextraspace': "1.5",
        'interval1': "Q1 2023",  # Example value, adjust according to actual data needs
        'interval2': "Q2 2023"  # Added in case this is also needed
    }

    # Populate template
    filled_template = populate_template(fund_report_template, data)

    # Generate PDF
    generate_pdf(filled_template, "PrimeFund_M_Report")


import re

def extract_placeholders(template):
    # Regular expression to find all placeholders in curly braces
    placeholders = re.findall(r'\{([^\{\}]+)\}', template)
    # Remove duplicates by converting to a set
    return set(placeholders)

def verify_placeholders(template, data_keys):
    placeholders = extract_placeholders(template)
    missing_keys = placeholders - data_keys
    if missing_keys:
        print("Missing data for placeholders:", missing_keys)
    else:
        print("All placeholders have corresponding data keys.")

config = load_config("config.yaml")
fund_report_template = config['fund_report_template']
# Example usage
template = fund_report_template

data = {
        'fundname': 'Prime Fund M',
        'report_date': datetime.now().strftime("%Y-%m-%d"),
        'fund_description': "This fund focuses on short-term securities.",
        'series_description': "The Series M targets stable returns with minimal risk.",
        'series_abbrev': "M",
        'benchmark': "Libor",
        'tgt_outperform': "50",
        'prev_pd_start': "2023-01-01",
        'this_pd_start': "2023-02-01",
        'prev_pd_return': "0.5%",
        'prev_pd_benchmark': "0.3%",
        'prev_pd_outperform': "20 bps",
        'this_pd_end': "2023-03-01",
        'this_pd_est_return': "0.6%",
        'benchmark_short': "Libor",
        'this_pd_est_outperform': "30 bps",
        'tablevstretch': "1.2",
        'return_table_plot': "Data for returns and plots goes here",
        'fund_size': "200M USD",
        'series_size': "50M USD",
        'lucid_aum': "1B USD",
        'rating': "AAA",
        'rating_org': "Moody's",
        'calc_frequency': "Monthly",
        'next_withdrawal_date': "2023-04-01",
        'next_notice_date': "2023-03-15",
        'min_invest': "10,000 USD",
        'wal': "30",
        'legal_fundname': "Prime Fund M LP",
        'fund_inception': "2020-01-01",
        'series_inception': "2020-02-01",
        'performance_graph': "Graph data or image path",
        'toptableextraspace': "1.5",
        'interval1': "Q1 2023",  # Example value, adjust according to actual data needs
        'interval2': "Q2 2023"  # Added in case this is also needed
    }

data_keys = set(data.keys())
verify_placeholders(template, data_keys)

#
# if __name__ == "__main__":
#     main()
