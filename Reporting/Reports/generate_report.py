from jinja2 import Environment, FileSystemLoader
from datetime import datetime

def generate_report(data):
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('prime_fund_m_template.tex')
    return template.render(data)

if __name__ == '__main__':
    data = {
        'report_date': datetime.now().strftime('%B %d, %Y'),
        'series': 'M',
        'frequency': 'Monthly',
        'rating': 'AAA',
        'target_return_bps': '50-52',
        'prev_period_start': 'Mar 14',
        'prev_period_end': 'Apr 18',
        'prev_period_return': '5.83',
        'prev_period_spread': '50',
        'curr_period_start': 'Apr 18',
        'curr_period_end': 'May 16',
        'curr_period_return': '5.82',
        'curr_period_spread': '50',
        'net_returns': [
            {'series': 'Prime Series M', 'prev_period_return': '5.83', 'prev_period_spread': '50', 'three_month_return': '5.82', 'three_month_spread': '50', 'one_year_return': '5.83', 'one_year_spread': '50'},
            {'series': '1m SOFR', 'prev_period_return': '5.33', 'prev_period_spread': '', 'three_month_return': '5.32', 'three_month_spread': '', 'one_year_return': '5.33', 'one_year_spread': ''},
            {'series': '1m A1/P1 CP', 'prev_period_return': '5.30', 'prev_period_spread': '',
             'three_month_return': '5.29', 'three_month_spread': '', 'one_year_return': '5.30', 'one_year_spread': ''}
        ],
        'fund_size': '1.2',
        'series_size': '500',
        'lucid_aum': '2.5',
        'series_rating': 'AAA',
        'series_withdrawal': 'Monthly',
        'next_withdrawal': 'May 16, 2023',
        'next_notice_date': 'May 9, 2023',
        'prime_series_m_coords': [
            {'date': '2023-01-12', 'value': '5.83'},
            {'date': '2023-02-09', 'value': '5.82'},
            {'date': '2023-03-16', 'value': '5.83'},
            {'date': '2023-04-13', 'value': '5.82'}
        ],
        'sofr_coords': [
            {'date': '2023-01-12', 'value': '5.33'},
            {'date': '2023-02-09', 'value': '5.32'},
            {'date': '2023-03-16', 'value': '5.33'},
            {'date': '2023-04-13', 'value': '5.32'}
        ],
        'cp_coords': [
            {'date': '2023-01-12', 'value': '5.30'},
            {'date': '2023-02-09', 'value': '5.29'},
            {'date': '2023-03-16', 'value': '5.30'},
            {'date': '2023-04-13', 'value': '5.29'}
        ],
        'benchmark': 60,
        'tgt_outperform_bps': 50,
    }

    report = generate_report(data)
    with open('PrimeFund_M_report.tex', 'w') as f:
        f.write(report)