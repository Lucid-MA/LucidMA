# Constants for fund names
USG = "USG"
PRIME = "PRIME"

# Constants for series types
SERIES_M = "M"
SERIES_Q1 = "Q1"
SERIES_QX = "QX"
SERIES_MIG = "MIG"

# Constants for day count conventions
DAY_COUNT_360 = 360
DAY_COUNT_365 = 365

# Constants for max characters in graph descriptions
MAX_CHARS = 385

# Constants for error messages
ERROR_FETCHING_DATA = "Error fetching data."
fund_report_template = r"""
\documentclass[9pt]{{article}}
\usepackage[T1]{{fontenc}}
\hfuzz=27pt 
\usepackage{{lmodern}}
\usepackage[dvipsnames,table,xcdraw]{{xcolor}}
\definecolor{{lucid_blue}}{{RGB}}{{0,18,82}}
\definecolor{{light_grey}}{{RGB}}{{198,198,198}}
\definecolor{{dark_red}}{{RGB}}{{144,8,8}}
\definecolor{{dark_color}}{{RGB}}{{9,143,68}}
\definecolor{{dark_grey}}{{RGB}}{{194,194,214}}
\usepackage[margin=0.7in,headsep=0.1in]{{geometry}}
\usepackage{{fancyhdr}}
\usepackage{{graphicx}}
\usepackage{{listings}}
\usepackage{{colortbl}}
\usepackage{{boldline}}
\graphicspath{{ {{images/}} }}
\pagestyle{{fancy}}
\usepackage{{adjustbox}}
\usepackage{{pgfplots}}
\renewcommand{{\familydefault}}{{\sfdefault}} % make font nice non bitmap
\renewcommand*{{\thepage}}{{\small\arabic{{page}}}}
\usepgfplotslibrary{{dateplot}}
\def\mywidth{{17.6cm}}
\def\halfwidthb{{12.4cm}}
\def\quarterwidthb{{6cm}}
\def\quarterwidth{{4.35cm}}
\def\eighthwidth{{2.175cm}}
\def\quarterwidtha{{3.41cm}}
\def\eighthwidtha{{1.2cm}}
\def\lesswidth{{10.9cm}}
\usetikzlibrary{{shadows,shadows.blur,shapes.geometric}}
\pgfplotsset{{compat=1.8}}
\pgfplotsset{{every axis/.append style={{
					label style={{font=\tiny}},
					tick label style={{font=\tiny}}  
					}}}}
\lhead{{\includegraphics[width=9cm]{{lucid_logo.png}}}}
\rhead{{Report Date: {report_date}}}
\setlength{{\columnsep}}{{3em}}
\begin{{document}}
\twocolumn[{{
\begin{{center}}
\renewcommand{{\arraystretch}}{{1.5}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor{{lucid_blue}}}}m{{\mywidth}} !{{\color{{light_grey}}\vrule}}}}
{{\color[HTML]{{FFFFFF}} \large \textbf{{Monthly Report}}}}\\
\end{{tabular}}
\end{{center}}

\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor{{lucid_blue}}}}l !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{\quarterwidthb}} !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}l !{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
{{\color[HTML]{{FFFFFF}} \textbf{{Program Series}}}} & \multicolumn{{2}}{{p{{\halfwidthb}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{Lucid {fundname} - Series {series_abbrev}}}}} \\[1.5mm]
{{\color[HTML]{{FFFFFF}} \textbf{{Objective and Strategy}}}} & \multicolumn{{2}}{{p{{\halfwidthb}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}} {fund_description} 

{series_description}}} \\[{toptableextraspace}]
{{\color[HTML]{{FFFFFF}} \textbf{{Current Target Return}}\textsuperscript{{1}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{{benchmark} + {tgt_outperform} bps}}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Previous Period Return}}}} & {prev_pd_start} - {this_pd_start} & \textbf{{{prev_pd_return}}} ({prev_pd_benchmark} + {prev_pd_outperform}) \\ 
{{\color[HTML]{{FFFFFF}} \textbf{{Current Period Est'd Return}}\textsuperscript{{2}}}} & {this_pd_start} - {this_pd_end} & \textbf{{{this_pd_est_return}}}  ({benchmark_short} + {this_pd_est_outperform} bps) \\ \arrayrulecolor{{light_grey}}\hline
\end{{tabular}}

\begin{{center}}
\noindent \renewcommand{{\arraystretch}}{{{tablevstretch}}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}m{{5.2cm}}!{{\color{{light_grey}}\vrule}}m{{\eighthwidth}}c!{{\color{{light_grey}}\vrule}}m{{\eighthwidtha}}c!{{\color{{light_grey}}\vrule}}m{{\eighthwidtha}}c!{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
\rowcolor{{lucid_blue}} 
{{\color[HTML]{{FFFFFF}} \textbf{{Net Returns}}\textsuperscript{{3}}}}               & \multicolumn{{2}}{{m{{\quarterwidth}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}\centering {{\color[HTML]{{FFFFFF}} \textbf{{Previous Period}}}}}} & \multicolumn{{2}}{{m{{\quarterwidtha}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}\centering {{\color[HTML]{{FFFFFF}} \textbf{{{interval1}}}}}}} & \multicolumn{{2}}{{m{{\quarterwidtha}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}\centering {{\color[HTML]{{FFFFFF}} \textbf{{{interval2}}}}}}} \\
\rowcolor{{lucid_blue}}  
{{\color[HTML]{{FFFFFF}} Series / Comparables}} & {{\color[HTML]{{FFFFFF}} Return}}                & {{\color[HTML]{{FFFFFF}} Spread}}               & {{\color[HTML]{{FFFFFF}} Return\textsuperscript{{1}}}}        & {{\color[HTML]{{FFFFFF}} Spread}}        & {{\color[HTML]{{FFFFFF}} Return\textsuperscript{{1}}}}       & {{\color[HTML]{{FFFFFF}} Spread}}       \\ \arrayrulecolor{{light_grey}}\hline
{return_table_plot}
\end{{tabular}}
\end{{center}}
}}]
\hfill \break
\hfill \break

\noindent \renewcommand{{\arraystretch}}{{{descstretch}}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{3.7cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{4cm}}!{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
\multicolumn{{2}}{{!{{\color{{light_grey}}\vrule}}l!{{\color{{light_grey}}\vrule}}}}{{\rowcolor{{lucid_blue}}{{ \color[HTML]{{FFFFFF}}\textbf{{Fund and Series Details\textsuperscript{{4}}}}}}}} \\
Fund Size & {fund_size}\\
Series Size & {series_size}\\
Lucid AUM & {lucid_aum}\\
Series Rating & {rating} by {rating_org}\\
Series Withdrawal & {calc_frequency}\\
Next Withdrawal & {next_withdrawal_date}\\
Next Notice Date & {next_notice_date}\\
Min Investment & {min_invest}\\
Current WAL & {wal} days\\
\noindent\parbox[b]{{\hsize}}{{\vspace{{1mm}}Current Max Limit on all Series Assets}} & {next_withdrawal_date}\\[-1mm]
Fund Entity & {legal_fundname}\\
Fund Inception & {fund_inception}\\
Series Inception & {series_inception}\\ \arrayrulecolor{{light_grey}}\hline
\end{{tabular}}
\hspace*{{-0.2cm}}\begin{{tabular}}{{p{{8.45cm}}}}
\textit{{\scriptsize Please see fund Offering Memorandum and related documents for complete terms and Important Disclaimer attached.}}
\end{{tabular}}


\begin{{figure}}
\centering
\noindent\renewcommand{{\arraystretch}}{{{pcompstretch}}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{3.5cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}c
>{{\columncolor[HTML]{{EFEFEF}}}}c!{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
\multicolumn{{3}}{{!{{\color{{light_grey}}\vrule}}l!{{\color{{light_grey}}\vrule}}}}{{\rowcolor{{lucid_blue}}{{ \color[HTML]{{FFFFFF}}\textbf{{Portfolio Composition\textsuperscript{{5}}}}}}}} \\
\textbf{{Series Assets}} & \textbf{{\% Portfolio}} & \textbf{{O/C Rate}}\\
{usg_aaa_cat} & {alloc_aaa} & {oc_aaa} \\
{addl_coll_breakdown}
T-Bills; Gov't MMF & {alloc_tbills} & {oc_tbills} \\ \cline{{2-2}} \cline{{3-3}} 
\textbf{{Total}} & {alloc_total} & \textbf{{{oc_total}}} \\\arrayrulecolor{{light_grey}}\hline
\end{{tabular}}

% \tikzfading[name=fade down,
%     top color=transparent!30,
%     bottom color=transparent!0]

{performance_graph}
\end{{figure}}


\onecolumn



\pagebreak 

\footnotesize
\noindent\textbf{{\color{{lucid_blue}}Notes}}

\begin{{enumerate}}
\item Target returns based on the program manager's estimate of the projected returns for the respective series based on current market conditions. 

\item Current return (estimated) is based on the rates of the invested series portfolio as of the current period start date.  Actual period return based on the final net returns of portfolio.   

\item Annualized net returns of Fund Series and comparables are for the entirety of each period and are quoted on an Act/360 basis for Lucid Prime Series and Act/365 for Lucid USG series. Any interperiod subscriptions will have different returns based upon the respective interperiod portfolio investments and allocations. Net returns include the applicable series expense ratio and include any management fee waivers or maximum expense caps. Historical returns assume reinvestment at the applicable Fund Series, Libor, T-Bill or MMF Index rate at the end of each period.  Money Market index returns based on the average of the daily rates for the respective period. SOFR is the term reference rate for the applicable period (e.g. 1m or 3m) as published by the CME Group. {exp_rat_footnote}

\item All Fund details as of the last period end date. Fund accepts new subscriptions and redemptions on each Withdrawal Date.  Manager may accept subscriptions on any other day with approval, as fully described in the private offering memorandum.

\item Portfolio composition and Over-Collateralization Rate (``O/C Rate'') of the repo investments as of the business day prior to the last day of the most recent period. O/C Rate equals the market value of the collateral as a proportion of the respective repo investments. Eligible repo collateral details and classifications for the Series as fully described in the private offering memorandum.

\end{{enumerate}}

\noindent\textbf{{\color{{lucid_blue}}Important Disclaimer}}

\scriptsize

\noindent This material has been prepared by Lucid Management and Capital Partners LP or one of its affiliates, principals or advisors (``Lucid'') and may contain ``forward-looking statements'' which are based on Lucid's beliefs, as well as on a number of assumptions concerning future events, based on information currently available to Lucid. Readers are cautioned not to put undue reliance on such forward-looking statements, which are not a guarantee of future performance, and are subject to a number of uncertainties and other factors, many of which are outside Lucid's control, which could cause actual results to differ materially from such statements. This material is for distribution only under such circumstances as may be permitted by applicable law and it is solely intended for qualified institutions and individuals with existing relationships with Lucid, its affiliates or advisers.  It has no regard to the specific investment objectives, financial situation or particular needs of any recipient. It is published solely for informational and discussion purposes only and is not to be construed as a solicitation or an offer to buy or sell any securities, related financial instruments, actual fund or specific transaction. No representation or warranty, either express or implied, is provided in relation to the accuracy, completeness or reliability of the information contained herein, nor is it intended to be a complete statement or summary of the securities, markets or developments referred to in the materials.  It should not be regarded by recipients as a substitute for the exercise of their own judgment. Any opinions expressed in this material are subject to change without notice and may differ or be contrary to opinions expressed by other business areas or groups of Lucid as a result of using different assumptions and criteria. Lucid is under no obligation to update or keep current the information contained herein. Lucid, its partners, officers and employees' or clients may have or have had interests or long or short positions in the securities or other financial instruments referred to herein and may at any time make purchases and/or sales in them as principal or agent. Neither Lucid nor any of its affiliates, nor any of Lucid or any of its affiliates, partners, employees or agents accepts any liability for any loss or damage arising out of the use of all or any part of this material. Money market investments including repurchase agreements are not suitable for all investors. Past performance is not necessarily indicative of future results.  Prior to entering into a transaction you should consult with your own legal, regulatory, tax, financial and accounting advisers to the extent you deem necessary to make your own investment, hedging and trading decisions. Any transaction between you and Lucid will be subject to the detailed provisions of a private placement memorandum or a managed account agreement relating to that transaction. Additional information will be made available upon request. The indicative information in this document (the ``Information'') are provided to you for information purposes only and may not be complete. Any redistribution of this document without express written consent of Lucid is prohibited.  No part of this material may be reproduced in any form, or referred to in any publication, without express written consent of Lucid.  This presentation should only be considered current as of the date of the publication without regard to the date on which you may have accessed or received the information. Unless required by law or specifically agreed in writing, we have no obligation to continue to provide to you the Information, and we may cease doing so at any time in our sole discretion. \underline{{\textit{{Targeted Return Disclosure}}}} \textit{{The targeted returns included in this presentation are not intended as, and must not be regarded as, a representation, warranty or prediction that any Fund (or series thereof) will achieve any particular rate of return over any particular time period or that any Fund (or series thereof) will not incur losses. Although Lucid believes, based on these factors, that the referenced return targets are reasonable, return targets are subject to inherent limitations including, without limitation, the fact they cannot take into account the impact of future economic events on future trading and investment decisions. These events may include changes in interest rates and/or benchmarks greater than those occurring within the historical time period examined when developing the return targets, or future changes in laws or regulations. All targeted returns are net of Lucid's anticipated fees and expenses.}}

\vspace{{1mm}}
\noindent SEC ADV Part 2 firm brochure: \underline{{https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd\_iapd\_Brochure.aspx?BRCHR\_VRSN\_ID=903911}}}}


\scriptsize
\begin{{center}}
\renewcommand{{\arraystretch}}{{1.2}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{8cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}l !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{9cm}}  !{{\color{{light_grey}}\vrule}}}}
\multicolumn{{3}}{{!{{\color{{light_grey}}\vrule}}l!{{\color{{light_grey}}\vrule}}}}{{\rowcolor{{lucid_blue}}{{ \color[HTML]{{FFFFFF}}\textbf{{Contact Information}}}}}} \\ \arrayrulecolor{{light_grey}}\hline
\textbf{{Investment Manager}} &  & \textbf{{For Investors (Subscriptions \& Withdrawals)}} \\\arrayrulecolor{{light_grey}}\hline
Lucid Management and Capital Partners LP &  & \underline{{Lucid.IR@sscinc.com}} with copy to: \\
295 Madison Avenue, 39th Floor &  & \underline{{operations@lucidma.com}} \\
New York, New York 10017 &  & \\
T: +1-212-551-1702 &  &  \\
Investor Relations: \underline{{carolina.siles@lucidma.com}} &  & \\
\textbf{{Fund Auditor:}} KPMG & & \textbf{{Fund Custodian:}} Bank of NY Mellon\\ \arrayrulecolor{{light_grey}}\hline
\end{{tabular}}
\end{{center}}
\end{{document}}
"""
note_report_template = r"""

\documentclass[9pt]{{article}}
\usepackage[T1]{{fontenc}}
\hfuzz=27pt 
\usepackage{{lmodern}}
\usepackage[dvipsnames,table,xcdraw]{{xcolor}}
\definecolor{{lucid_blue}}{{RGB}}{{0,18,82}}
\definecolor{{light_grey}}{{RGB}}{{198,198,198}}
\definecolor{{dark_red}}{{RGB}}{{144,8,8}}
\definecolor{{dark_color}}{{RGB}}{{9,143,68}}
\definecolor{{dark_grey}}{{RGB}}{{194,194,214}}
\definecolor{{darker_grey}}{{RGB}}{{143,143,143}}
\usepackage[margin=0.7in,headsep=0.1in]{{geometry}}
\usepackage{{fancyhdr}}
\usepackage{{graphicx}}
\usepackage{{subfig}}
\usepackage{{colortbl}}
\usepackage{{listings}}
\usepackage{{boldline}}
\usepackage{{ragged2e}}
\graphicspath{{ {{images/}} }}
\pagestyle{{fancy}}
\usepackage{{adjustbox}}
\usepackage{{pgfplots}}
\renewcommand{{\familydefault}}{{\sfdefault}} % make font nice non bitmap
\renewcommand*{{\thepage}}{{\small\arabic{{page}}}}
\usepgfplotslibrary{{dateplot}}
\def\mywidth{{17.6cm}}
\def\halfwidthb{{11.2cm}}
\def\quarterwidthb{{4cm}}
\def\quarterwidth{{4.35cm}}
\def\eighthwidth{{2.175cm}}
\def\quarterwidtha{{3.41cm}}
\def\eighthwidtha{{1.2cm}}
\usetikzlibrary{{shadows,shadows.blur,shapes.geometric}}
\pgfplotsset{{compat=1.8}}
\pgfplotsset{{every axis/.append style={{
					label style={{font=\tiny}},
					tick label style={{font=\tiny}}  
					}}}}
\lhead{{\includegraphics[width=9cm]{{lucid_logo.png}}}}
\rhead{{Report Date: {report_date}}}
\setlength{{\columnsep}}{{3em}}
\begin{{document}}
\

\noindent\renewcommand{{\arraystretch}}{{1.5}}\begin{{tabular}}{{
>{{\columncolor[HTML]{{8F8F8F}}}}p{{8.3cm}} 
>{{\columncolor[HTML]{{8F8F8F}}}}p{{5.5cm}} 
>{{\columncolor[HTML]{{8F8F8F}}}}r}}
\textbf{{\large Lucid Medium Term Notes}} &  & \textbf{{Noteholder Report}} \\
\end{{tabular}}

\begin{{center}}
\renewcommand{{\arraystretch}}{{1.3}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor{{lucid_blue}}}}l !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{\quarterwidthb}} !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}l !{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
%\cline{{2-3}}


{{\color[HTML]{{FFFFFF}} \textbf{{Program Series}}}} & \multicolumn{{2}}{{p{{11.78cm}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}} 
\textbf{{{reporting_series_name}}}
}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Issuer}}}} & \multicolumn{{2}}{{p{{11.78cm}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}} 
{issuer_name}
}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Redemption \& Coupon Frequency}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{{frequency}}}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Rating}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{{rating}}} by {rating_org}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Current Target Return of Notes}}\textsuperscript{{1}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{{benchmark} + {tgt_outperform} bps}}}}\\
{{\color[HTML]{{FFFFFF}} \textbf{{Previous Coupon Period}}}} & {prev_pd_start} - {this_pd_start} & \textbf{{{prev_pd_return}}} ({prev_pd_benchmark} + {prev_pd_outperform}) \\
{{\color[HTML]{{FFFFFF}} \textbf{{Current Period Est'd Coupon}}\textsuperscript{{2}}}} & {this_pd_start} - {this_pd_end} & \textbf{{{this_pd_est_return}}}  ({benchmark_short} + {this_pd_est_outperform} bps) \\ \arrayrulecolor{{light_grey}}\hline
\end{{tabular}}
\end{{center}}

\noindent{{\color{{lucid_blue}}\textbf{{Historical Performance of Program Series vs Benchmarks}} \textit{{(all-in net returns)}}\textsuperscript{{3}}}}

\noindent\renewcommand{{\arraystretch}}{{1.3}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}m{{5.2cm}}!{{\color{{light_grey}}\vrule}}m{{\eighthwidth}}c!{{\color{{light_grey}}\vrule}}m{{\eighthwidtha}}c!{{\color{{light_grey}}\vrule}}m{{\eighthwidtha}}c!{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
\rowcolor{{lucid_blue}} 
{{\color[HTML]{{FFFFFF}} \textbf{{ Series / Comparables}}}}               & \multicolumn{{2}}{{m{{\quarterwidth}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}\centering {{\color[HTML]{{FFFFFF}} \textbf{{Previous Period}}}}}} & \multicolumn{{2}}{{m{{\quarterwidtha}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}\centering {{\color[HTML]{{FFFFFF}} \textbf{{{interval1}}}}}}} & \multicolumn{{2}}{{m{{\quarterwidtha}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}\centering {{\color[HTML]{{FFFFFF}} \textbf{{{interval2}}}}}}} \\ 
\rowcolor{{lucid_blue}}  
{{\color[HTML]{{FFFFFF}}}} & {{\color[HTML]{{FFFFFF}} Return\textsuperscript{{1}}}}                & {{\color[HTML]{{FFFFFF}} Spread}}               & {{\color[HTML]{{FFFFFF}} Return\textsuperscript{{1}}}}        & {{\color[HTML]{{FFFFFF}} Spread}}        & {{\color[HTML]{{FFFFFF}} Return\textsuperscript{{1}}}}       & {{\color[HTML]{{FFFFFF}} Spread}}       \\ \arrayrulecolor{{light_grey}}\hline
{return_table_plot}
\end{{tabular}}

\vspace{{.8cm}}

\noindent\adjustbox{{valign=t}}{{\begin{{minipage}}{{9.5cm}}
\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor{{lucid_blue}}}}m{{8.2cm}} !{{\color{{light_grey}}\vrule}}}}
{{\color[HTML]{{FFFFFF}} \small \textbf{{Most Recent Period Returns vs Benchmarks\textsuperscript{{3}}}}}}\\
\end{{tabular}}
{performance_graph}
\end{{minipage}}}}%
\hfill
\adjustbox{{valign=t}}{{\begin{{minipage}}[t]{{15cm}}
	{colltable}
\end{{minipage}}}}

\begin{{center}}
\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{5.05cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{3.05cm}} !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{4.05cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{4.35cm}} !{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
\multicolumn{{4}}{{!{{\color{{light_grey}}\vrule}}l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor{{lucid_blue}}{{\color[HTML]{{FFFFFF}}\textbf{{Program Overview\textsuperscript{{5}}}}}}}} \\ \arrayrulecolor{{light_grey}}\hline
\textbf{{Related Fund Series Size}} & \textbf{{{series_size}}} & Issuing \& Paying Agent & Bank of NY Mellon \\ 
\textbf{{Lucid {fundname} Program Size}} & \textbf{{{fund_size}}} & Collateral Agent & Bank of NY Mellon \\ 
\textbf{{Lucid Platform AUM}} & \textbf{{{lucid_aum}}} & Auditor & KPMG \\ 
{fundname} Program Inception & {fund_inception} & & \\ \arrayrulecolor{{light_grey}}\hline
\end{{tabular}}
\end{{center}}

\begin{{center}}
	\noindent{{\small Please see page 2 for Coupons by CUSIP, program documents for complete terms, and Important Disclaimer attached.}}
\end{{center}}


\pagebreak 

\noindent\renewcommand{{\arraystretch}}{{1.5}}\begin{{tabular}}{{
>{{\columncolor[HTML]{{8F8F8F}}}}m{{\mywidth}}}}
{{\large \textbf{{Coupons by CUSIP}}}}\\
\end{{tabular}}


\begin{{center}}
\noindent\renewcommand{{\arraystretch}}{{1.3}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
>{{\columncolor{{lucid_blue}}}}l !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1cm}} !{{\color{{light_grey}}\vrule}}
>{{\columncolor[HTML]{{EFEFEF}}}}l !{{\color{{light_grey}}\vrule}}}}
\arrayrulecolor{{light_grey}}\hline
%\cline{{2-3}}
{{\color[HTML]{{FFFFFF}} \textbf{{CUSIP}}}} & \multicolumn{{2}}{{p{{11.56cm}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}} 
\textbf{{{cusip}}}
}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Note Series}}}} & \multicolumn{{2}}{{p{{11.56cm}}!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}} 
\textbf{{{note_abbrev}}} (Secured by related fund series {fund_abbrev})
}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Current Principal}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{{principal_outstanding}}}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Original Issue Date}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}{issue_date}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Final Maturity Date}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}{maturity_date}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Next Coupon Payment Date}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}{pd_end_date_long}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Next Redemption Date (Put Date)}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}\textbf{{{pd_end_date_long}}}}} \\
{{\color[HTML]{{FFFFFF}} \textbf{{Next Notice Date for Redemption}}}} & \multicolumn{{2}}{{l!{{\color{{light_grey}}\vrule}}}}{{\cellcolor[HTML]{{EFEFEF}}{next_notice_date}}} \\ \arrayrulecolor{{light_grey}}\hline
\end{{tabular}}
\end{{center}}

\begin{{center}}{{\footnotesize
\noindent\begin{{tabular}}{{
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1.45cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1.45cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1.70cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1.6cm}} 
>{{\columncolor[HTML]{{EFEFEF}}\RaggedLeft\arraybackslash}}p{{2cm}} 
>{{\columncolor[HTML]{{EFEFEF}}\RaggedLeft\arraybackslash}}p{{1.52cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1.52cm}} 
>{{\columncolor[HTML]{{EFEFEF}}\RaggedLeft\arraybackslash}}p{{1.64cm}} 
>{{\columncolor[HTML]{{EFEFEF}}}}p{{1.40cm}} }}
\textbf{{Interest Period Start}} & \textbf{{Interest Period End}} & \textbf{{Interest Rate}} & \textbf{{Benchmark Spread}} & \RaggedRight\arraybackslash\textbf{{Note Series Principal}} & \RaggedRight\arraybackslash\textbf{{Interest Paid}} & \textbf{{Interest Payment Date}} & \RaggedRight\arraybackslash\textbf{{Related Fund Cap. Account}} & \textbf{{Collateral O/C Rate}} \\ \arrayrulecolor{{light_grey}}\hline
{coupon_plot}
\end{{tabular}}
}}\end{{center}}


{{\small
\color{{gray}}
	\noindent\textbf{{Note:}}  The Series Portfolio (which includes the Collateral Securities) is pledged to BNYM as the Collateral Agent for the Noteholders and equals the Note Principal Amount. A copy of the most recent capital account statement is available by request at \underline{{operations@lucidma.com}}.{rets_disclaimer_if_m1}
}}

\pagebreak

\footnotesize
\noindent\textbf{{\color{{lucid_blue}}Report Notes}}

\begin{{enumerate}}
\item Target returns based on the program manager's estimate of the projected returns for the respective series based on current market conditions. 

\item Current coupon (estimated) is based on the rates of the invested portfolio of the Related Fund Interest as of the current period start date.  Actual rate set in arrears based on the final net returns of portfolio.  

\item Annualized net returns quoted on an Act/360 basis after all program costs. Historical returns assume reinvestment at the respective Series return, money market index, Libor rate or T-Bill Index rate at the end of each period.  Libor is the applicable USD London Interbank offered Rate for each calculation period, as published by the ICE Benchmark Administration Fixing. T-Bill is the offer rate for the T-Bills with a maturity matching the respective coupon period (or interpolated rate if the dates do not match). Crane Prime Institutional Money Market Index adjusted to an Actual/360 basis (for Prime notes) and Act/365 for USG Notes, based on the daily average for the periods. A1/P1 CP is dealer placed commercial paper corresponding to the period as published by Bloomberg. SOFR is the term reference rate for the applicable period (e.g. 1m or 3m) as published by the CME Group.

\item Over-Collateralization Rate (``O/C Rate'') of the repo investments securing the notes in the Related Fund Interest as of the business day prior to the end of the most recent period. O/C Rates will vary each period based on the specific risk characteristics of the collateral securities that meet the Lucid risk management standards. O/C Rate equals the market value of the collateral as a proportion of the respective repo investments. Eligible repo collateral details and classifications for the respective Series as fully described in the private offering memorandum. Historical Returns based on the investment program series performance since inception in the related fund series or notes; accordingly, specific Secured Notes that reference the series (e.g. M-1, M-2) purchased in the middle of coupon dates or issued after the inception date may have different returns based on inter-period issuances or different holding periods. Please review the specific interest rates for the respective secured note CUSIP on page 2 for the specific returns applicable to the note.

\item Program AUM based on the amounts invested in the series strategy through the Secured Notes or directly in the related fund entity (Lucid Prime Fund LLC).  For each program series (e.g. Series M), the returns of the Secured Notes and direct investments in the related fund are the same given the program structure. Lucid Platform AUM is the contracted assets under management as of the report date.

\end{{enumerate}}

\noindent Please refer to the Private Placement Memorandum and the Series Supplement for complete details. 

{{\color{{gray}} \noindent The SEC ADV Part 2 firm brochure on the administrator can be accessed via the the following link:

\noindent\underline{{https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd\_iapd\_Brochure.aspx?BRCHR\_VRSN\_ID=903911}}}}

\begin{{center}}\noindent\begin{{tabular}}{{p{{\textwidth}}}}
\rowcolor{{lucid_blue}} 
{{\color[HTML]{{FFFFFF}} \textbf{{Contact Information}}}} \\
\rowcolor[HTML]{{EFEFEF}} 
 \\
\rowcolor[HTML]{{EFEFEF}} 
\textbf{{{issuer_name}, issuer}} \\
\rowcolor[HTML]{{EFEFEF}} 
 c/o Lucid Management and Capital Partners as administrator\\
\rowcolor[HTML]{{EFEFEF}} 
 295 Madison Avenue, 39th Floor\\
\rowcolor[HTML]{{EFEFEF}} 
 New York, NY 10017\\
\rowcolor[HTML]{{EFEFEF}} 
 T: +1-212-551-1704\\
\rowcolor[HTML]{{EFEFEF}} 
 Redemption Notices: \underline{{operations@lucidma.com}}\\
\rowcolor[HTML]{{EFEFEF}} 
 Investor Relations: \underline{{carolina.siles@lucidma.com}}\\
\rowcolor[HTML]{{EFEFEF}} 
 \\
\rowcolor[HTML]{{EFEFEF}} 
\textbf{{Bank of New York Mellon, Issuing \& Paying Agent / Collateral Agent}} \\
\rowcolor[HTML]{{EFEFEF}} 
 240 Greenwich Street, Suite 7E\\
\rowcolor[HTML]{{EFEFEF}} 
 New York, NY 10286\\
\rowcolor[HTML]{{EFEFEF}} 
 T: +1-212-815-5837\\
\rowcolor[HTML]{{EFEFEF}} 
 \underline{{audrey.williams@bnymellon.com}}\\
\rowcolor[HTML]{{EFEFEF}} 
 \\
\rowcolor[HTML]{{EFEFEF}} 
 \textbf{{KPMG, Auditor and Tax Reporting}}\\
\rowcolor[HTML]{{EFEFEF}} 
 345 Park Avenue\\
\rowcolor[HTML]{{EFEFEF}} 
 New York, NY 10154\\
\rowcolor[HTML]{{EFEFEF}} 
 \\
\rowcolor[HTML]{{EFEFEF}} 

\end{{tabular}}
\end{{center}}

\pagebreak

 \ \\
  \ \\
  \ \\
   \ \\
  \ \\
  \ \\

\noindent\textbf{{\color{{lucid_blue}}Important Disclaimer}}

\small

\noindent This material has been prepared by Lucid Management and Capital Partners LP or one of its affiliates, principals or advisors (``Lucid'') and may contain ``forward-looking statements'' which are based on Lucid's beliefs, as well as on a number of assumptions concerning future events, based on information currently available to Lucid. Readers are cautioned not to put undue reliance on such forward-looking statements, which are not a guarantee of future performance, and are subject to a number of uncertainties and other factors, many of which are outside Lucid's control, which could cause actual results to differ materially from such statements.

\noindent This material is for distribution only under such circumstances as may be permitted by applicable law and it is solely intended for qualified institutions and individuals with existing relationships with Lucid, its affiliates or advisers.  It has no regard to the specific investment objectives, financial situation or particular needs of any recipient. It is published solely for informational and discussion purposes only and is not to be construed as a solicitation or an offer to buy or sell any securities, related financial instruments, actual fund or specific transaction. No representation or warranty, either express or implied, is provided in relation to the accuracy, completeness or reliability of the information contained herein, nor is it intended to be a complete statement or summary of the securities, markets or developments referred to in the materials.  It should not be regarded by recipients as a substitute for the exercise of their own judgment. Any opinions expressed in this material are subject to change without notice and may differ or be contrary to opinions expressed by other business areas or groups of Lucid as a result of using different assumptions and criteria. Lucid is under no obligation to update or keep current the information contained herein. Lucid, its partners, officers and employees' or clients may have or have had interests or long or short positions in the securities or other financial instruments referred to herein and may at any time make purchases and/or sales in them as principal or agent. Neither Lucid nor any of its affiliates, nor any of Lucid or any of its affiliates, partners, employees or agents accepts any liability for any loss or damage arising out of the use of all or any part of this material.

\noindent Money market investments including repurchase agreements are not suitable for all investors. Past performance is not necessarily indicative of future results.  Prior to entering into a transaction you should consult with your own legal, regulatory, tax, financial and accounting advisers to the extent you deem necessary to make your own investment, hedging and trading decisions. Any transaction between you and Lucid will be subject to the detailed provisions of a private placement memorandum or a managed account agreement relating to that transaction. Additional information will be made available upon request.

\noindent The indicative information in this document (the ``Information'') are provided to you for information purposes only and may not be complete. Any redistribution of this document without express written consent of Lucid is prohibited.  No part of this material may be reproduced in any form, or referred to in any publication, without express written consent of Lucid.  This presentation should only be considered current as of the date of the publication without regard to the date on which you may have accessed or received the information. Unless required by law or specifically agreed in writing, we have no obligation to continue to provide to you the Information, and we may cease doing so at any time in our sole discretion.

\noindent\underline{{\textit{{Targeted Return Disclosure}}}}

\noindent\textit{{The targeted returns included in this presentation are not intended as, and must not be regarded as, a representation, warranty or prediction that any Fund (or series thereof) will achieve any particular rate of return over any particular time period or that any Fund (or series thereof) will not incur losses. Although Lucid believes, based on these factors, that the referenced return targets are reasonable, return targets are subject to inherent limitations including, without limitation, the fact they cannot take into account the impact of future economic events on future trading and investment decisions. These events may include changes in interest rates and/or benchmarks greater than those occurring within the historical time period examined when developing the return targets, or future changes in laws or regulations. All targeted returns are net of Lucid's anticipated fees and expenses.}}

\end{{document}}

"""

performance_graph_template_usg = r"""
			  \hspace*{{{graphhspace}cm}}\resizebox {{{graphwidth}}} {{{graphheight}}} {{\begin{{tikzpicture}}
		\begin{{axis}}[
			title style = {{font = \small}},
			axis line style = {{light_grey}},{title}
			date coordinates in=x, date ZERO={zero_date},
			xticklabel=\month/\day/\year,  
			ymin={min_return}, ymax={max_return}, %MAXRETURN HERE
			legend cell align = {{left}},
			legend style={{at={{(0.3,1)}},
			  anchor=north east, font=\tiny, draw=none,fill=none}},
			  x={graphx}mm, %CHANGE THIS to tighten in graph, eg if quarterly
			bar width={graphbarwidth}mm, ybar=2pt, %bar width is width, ybar is space between
		   % symbolic x coords={{Firm 1, Firm 2, Firm 3, Firm 4, Firm 5}},
			xtick=data,
			x tick label style={{rotate=90,anchor=east,font=\tiny,/pgf/number format/assume math mode}},
				 yticklabel=\pgfmathparse{{\tick}}\pgfmathprintnumber{{\pgfmathresult}}\,\%,
			y tick label style = {{/pgf/number format/.cd,
					fixed,
					fixed zerofill,
					precision=2,
					/pgf/number format/assume math mode
			}},
			nodes near coords align={{vertical}},
			ytick distance=0.5,
			xtick pos=bottom,ytick pos=left,
			every node near coord/.append style={{font=\fontsize{{6}}{{6}}\selectfont,/pgf/number format/.cd,
					fixed,
					fixed zerofill,
					precision=2,/pgf/number format/assume math mode}},
			]
		%\addplot[ybar, nodes near coords, fill=blue] 
		\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
			coordinates {{
				{return_plot}
			}};
		\addplot[draw=dark_red,ultra thick,smooth] 
			coordinates {{
				{comp_a_plot}
			}};
		\addplot[draw=dark_color,ultra thick,smooth] 
			coordinates {{
				{comp_b_plot}
			}};
		\legend{{\hphantom{{A}}{fund_name} Series {series_abbrev},\hphantom{{A}}{comp_a},\hphantom{{A}}{comp_b}}}
		\end{{axis}}
			\end{{tikzpicture}}}}

			"""

performance_graph_template_other = r"""
		  \hspace*{{{graphhspace}cm}}\resizebox {{{graphwidth}}} {{{graphheight}}} {{\begin{{tikzpicture}}
	\begin{{axis}}[
		title style = {{font = \small}},
		axis line style = {{light_grey}},{title}
		date coordinates in=x, date ZERO={zero_date},
		xticklabel=\month/\day/\year,  
		ymin={min_return}, ymax={max_return}, %MAXRETURN HERE
		legend cell align = {{left}},
		legend style={{at={{(0.25,1)}},
		  anchor=north east, font=\tiny, draw=none,fill=none}},
		  x={graphx}mm, %CHANGE THIS to tighten in graph, eg if quarterly
		bar width={graphbarwidth}mm, ybar=2pt, %bar width is width, ybar is space between
	   % symbolic x coords={{Firm 1, Firm 2, Firm 3, Firm 4, Firm 5}},
		xtick=data,
		x tick label style={{rotate=90,anchor=east,font=\tiny,/pgf/number format/assume math mode}},
			 yticklabel=\pgfmathparse{{\tick}}\pgfmathprintnumber{{\pgfmathresult}}\,\%,
		y tick label style = {{/pgf/number format/.cd,
				fixed,
				fixed zerofill,
				precision=2,
				/pgf/number format/assume math mode
		}},
		nodes near coords align={{vertical}},
		ytick distance=0.5,
		xtick pos=bottom,ytick pos=left,
		every node near coord/.append style={{font=\fontsize{{6}}{{6}}\selectfont,/pgf/number format/.cd,
				fixed,
				fixed zerofill,
				precision=2,/pgf/number format/assume math mode}},
		]
	%\addplot[ybar, nodes near coords, fill=blue] 
	\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			{return_plot}
		}};
	\addplot[draw=dark_red,ultra thick,smooth] 
		coordinates {{
			{comp_a_plot}
		}};
	\addplot[draw=dark_color,ultra thick,smooth] 
		coordinates {{
			{comp_b_plot}
		}};
	\legend{{\hphantom{{A}}{fund_name} Series {series_abbrev},\hphantom{{A}}{comp_a},\hphantom{{A}}{comp_b}}}
	\end{{axis}}
		\end{{tikzpicture}}}}
	
		"""
