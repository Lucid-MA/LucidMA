from Reports.Constants import (
    performance_graph_template_usg,
    performance_graph_template_other,
)


def secured_by_from(f, s):
    if f == "USG":
        return "US Government backed (USG) securities only"
    else:
        if s == "M":
            return "Highly Rated Investment Grade Collateral securities (at least 75\\% must be rated between AAA and A- or USG securities)"
        elif s == "Q1":
            return "Investment Grade Collateral securities only"
        elif s == "QX":
            return "Investment Grade and BB rated Collateral securities (subject to a 50\\% limit on BB collateral)"
        else:
            return "Highly Rated Investment Grade Collateral securities"


def accs_since_start(ws, r_col, tstart_col, tend_col, end, start, daycount):
    if end < start:
        return 1
    try:
        r = float(ws[r_col + str(end)].value)
        dt = (ws[tend_col + str(end)].value - ws[tstart_col + str(end)].value).days
        return accs_since_start(
            ws, r_col, tstart_col, tend_col, end - 1, start, daycount
        ) * (1 + r * dt / daycount)
    except:
        print(
            "Breaking accrual recursion: "
            + ws.title
            + " "
            + r_col
            + str(start)
            + ":"
            + r_col
            + str(end)
        )
        return 1


def diff_period_rate(t1, t2, daycount, r1, r2):
    try:
        dt = (t2 - t1).days
        outp = ((1.0 * r2 / r1) - 1) * daycount / dt
        return form_as_percent(outp, 2)
    except:
        return "n/a"


def form_as_percent(val, rnd):
    try:
        if float(val) == 0:
            return "-"
        return ("{:." + str(rnd) + "f}").format(100 * val) + "\\%"
    except:
        return "n/a"


def xl_average(ws, col, start, end):
    sum = 0
    count = 0
    for row in range(start, end + 1):
        if ws[col + str(row)].value:
            sum = sum + ws[col + str(row)].value
            count = count + 1
    if count == 0:
        return 1
    return 1.0 * sum / count


maxchars = 385


def extraspacefromdesc(varistring):
    if len(varistring) > 500:
        return "9.4em"
    return "5.5em"


def heightmap(varistring):
    if len(varistring) > maxchars:
        return 6.676
    else:
        return 8


def stretches(varistring):
    if len(varistring) > 500:
        return [1.6, 1.5]  # prime fund m
    return [1.72, 2.55]  # usg fund m


def hspacemap(varistring, nbars):
    if len(varistring) > maxchars:
        if nbars == 8:
            return -0.77
        if nbars == 7:
            return -0.88
        return -0.77  # default here, if 16
    else:
        return -0.9  # default here, if 16


def xmap(varistring, nbars):
    # max 16
    nbars = min(nbars, 16)
    if len(varistring) > maxchars:
        return [
            1.7,
            1.7,
            1.7,
            1.40,
            0.942,
            0.674,
            0.558,
            0.471,
            0.395,
            0.35,
            0.103,
            0.103,
            0.255,
            0.234,
            0.215,
            0.198,
            0.188,
        ][nbars]
    else:
        return 0.150  # default here, if 16


def barwidthmap(varistring, nbars):
    # max 16
    nbars = min(nbars, 16)
    if len(varistring) > maxchars:
        return [8, 8, 8, 8, 8, 6, 6, 6, 3, 3, 3, 3, 3, 3, 2.5, 2.5, 2.0][nbars]
    else:
        return 2.5  # default here, if 16


def notehspacemap(nbars):
    return -0.79


def notexmap(nbars):
    if nbars == 10:
        return 0.284
    if nbars == 11:
        return 0.258
    if nbars == 12:
        return 0.231
    if nbars == 4:
        return 0.615
    return 0.17


def notebarwidthmap(nbars):
    if nbars == 10:
        return 4
    if nbars == 4 or nbars == 5:
        return 4
    return 2


def xl_max(fn, ws, col, start, end):
    if fn == "USG":
        return 3
    if fn == "Prime":
        return 3.3
    return 3


def tablevstretch(fund_name):
    if fund_name == "USG":
        return 1.2
    else:
        return 1


def coupon_plotify(ws, crow, daycount):
    outp = r""""""
    maxrows = 12
    startrow = max(7, crow - maxrows + 1)
    remaining_rows = max(0, maxrows - (crow + 2 - startrow))
    for i in range(startrow, crow + 2):
        addl = ws["E" + str(i)].value.strftime("%m/%d/%y") + " &"
        addl = (
            addl
            + r"""\textbf{{"""
            + ws["F" + str(i)].value.strftime("%m/%d/%y")
            + r"""}} &"""
        )
        addl = (
            addl
            + r"""\textbf{{"""
            + form_as_percent(ws[("N" if daycount == 360 else "O") + str(i)].value, 2)
            + (r"""{{\tiny (Est'd)}}""" if i == crow + 1 else "")
            + (
                "*"
                if ws["C12"].value == "Monthly1"
                and ws["E" + str(i)].value.strftime("%m/%d/%y") == "12/30/20"
                else ""
            )
            + r"""}} &"""
        )
        addl = (
            addl
            + benchmark_shorten(ws["U" + str(i)].value)
            + ("+" if int(10000 * ws["W" + str(i)].value) > 0 else "-")
            + str(int(abs(ws["X" + str(i)].value)))
            + " &"
        )
        addl = (
            addl
            + (r"\$" if i == startrow else r"\hphantom{{\$}}")
            + "{:,.0f}".format(ws["R" + str(i)].value)
            + " &"
        )
        addl = (
            addl
            + (
                r"\hphantom{{\$}}n/a"
                if i == crow + 1
                else (
                    (r"\$" if i == startrow else r"\hphantom{{\$}}")
                    + "{:,.2f}".format(ws["T" + str(i)].value)
                )
            )
            + " &"
        )
        addl = (
            addl
            + ws[("S" if i < crow + 1 else "F") + str(i)].value.strftime("%m/%d/%y")
            + " &"
        )
        addl = (
            addl
            + (r"\$" if i == startrow else r"\hphantom{{\$}}")
            + "{:,.0f}".format(ws["R" + str(i)].value)
            + " &"
        )
        addl = (
            addl
            + form_as_percent(ws["AP" + str(i)].value, 1)
            + (r" \\" if i <= crow + 1 or remaining_rows > 0 else " ")
        )
        outp = outp + addl

    for i in range(remaining_rows):
        addl = r"""
		"""
        addl = addl + (
            r"""& & & & & & & &      \\ """ if i < remaining_rows - 1 else ""
        )
        outp = (
            outp
            + addl
            + r"""

		"""
        )
    return outp


def hardcoded_exp_cap(f, s):
    if f.upper() == "USG":
        return 24.5
    if f.upper() == "PRIME":
        if s == "MIG":
            return 39.75
        if s == "Q1":
            return 49.75
        if s == "QX":
            return 49.75
    return 32.75  # default


def exp_rat_footnote(incl, cap, rat):
    if False:
        out = r"""Fund Series expense ratio currently capped at an all-in ratio of {cap} bps and can vary over time. The average expense ratio has been {rat} bps since inception."""
        return out.format(cap=cap, rat=rat)
    else:
        out = r"""Fund Series expense ratio currently capped at an all-in ratio of {cap} bps and can vary over time."""
        return out.format(cap=cap)


def wordify(val):
    try:
        prefix = 0
        if val >= 10**9:
            prefix = round(round(val, 9) / (10**9), 2)
            suffix = " billion"
        elif val >= 10**6:
            prefix = round(round(val, 6) / (10**6), 1)
            suffix = " million"
        elif val >= 10**3:
            prefix = int(round(val, 3) / (10**3))
            suffix = ",000"
        return r"\$" + str(prefix) + suffix
    except:
        return "n/a"


def wordify_aum(val):
    try:
        prefix = 0
        if val >= 10**9:
            prefix = round(round(val, 9) / (10**9), 1)
            suffix = " billion"
        elif val >= 10**6:
            prefix = round(round(val, 6) / (10**6), 1)
            suffix = " million"
        elif val >= 10**3:
            prefix = int(round(val, 3) / (10**3))
            suffix = ",000"
        return r"\$" + str(prefix) + suffix
    except:
        return "n/a"


def month_wordify(months):
    if months < 12:
        return str(int(months)) + " Months" if int(months) != 1 else " Month"
    else:
        return (
            str(int(months / 12))
            + (" Years" if int(months / 12) != 1 else " Year")
            + ("" if months % 12 == 0 else (str(months % 12) + " Months"))
        )


def benchmark_shorten(s):
    if s == None:
        print("benchmark none")
        return ""
    if "1" in s.upper() and "LIBOR" in s.upper():
        return "1mL"
    if "3" in s.upper() and "LIBOR" in s.upper():
        return "3mL"
    if "1" in s.upper() and "BILL" in s.upper():
        return "1m TB"
    if "3" in s.upper() and "BILL" in s.upper():
        return "3m TB"
    if "CRANE GOV" in s.upper():
        return "Crane Govt"
    if "CRANE PRIME" in s.upper():
        return "Crane Prime"
    return s


# TODO: Consider delete this if the below works
# def bps_spread(t, b):
#     try:
#         val = round(float(t[0 : t.index("\\")]) - float(b[0 : b.index("\\")]), 2)
#         return (
#             "-"
#             if int(abs(val) * 100) == 0
#             else (
#                 ("+" if int(val * 100) > 0 else "-") + str(int(abs(val) * 100)) + " bps"
#             )
#         )
#     except:
#         return "n/a"


def bps_spread(t, b):
    try:
        # Extract numeric parts from input strings
        t_value = float(t.split("\\")[0])
        b_value = float(b.split("\\")[0])

        # Calculate absolute difference
        diff = abs(t_value - b_value)

        # Format output string
        return f"+{int(diff * 100)} bps"
    except:
        return "n/a"


def issuer_from_fundname(f):
    if f == None:
        print("issuer none")
    if f.upper() == "USG":
        return "USG Assets LLC"
    if f.upper() == "PRIME":
        return "Prime Notes LLC"
    return f


def declare_ratings_org(r):
    if r == None:
        return ""
    if "EGAN JONES" in r.upper():
        return r + " (NAIC 1)"
    return r


def series_from_note(f, ent_name):
    if ent_name == "Monthly":
        return "M"
    elif ent_name == "Quarterly" or ent_name == "Quarterly1":
        return "Q1"
    elif ent_name == "Monthly1":
        return "M1"
    elif ent_name == "MonthlyIG":
        return "MIG"
    elif ent_name == "QuarterlyX":
        return "QX"
    elif ent_name == "Custom1":
        return "C1"
    else:
        return ""


def fund_inception_from_name(fund):
    if fund == "USG":
        return "June 29, 2017"
    if fund == "Prime":
        return "July 20, 2018"
        return "n/a"


def return_table_plot(
    fund_name,
    prev_pd_return,
    series_abbrev,
    r_this_1,
    r_this_2,
    comp_a,
    comp_b,
    comp_c,
    r_a,
    r_b,
    r_c,
    s_a_0,
    s_a_1,
    s_a_2,
    s_b_0,
    s_b_1,
    s_b_2,
    s_c_0,
    s_c_1,
    s_c_2,
):
    if fund_name == "USG":
        out = r"""
	\textbf{{Lucid {fund_name} - Series {series_abbrev}}}                    & \textbf{{{prev_pd_return}}}                              & \textbf{{-}}                                  & \textbf{{{r_this_1}}}                               & \textbf{{-}}                           & \textbf{{{r_this_2}}}                             & \textbf{{-}}                          \\
{comp_a}                       & {r_a_0}                                       & \textbf{{{s_a_0}}}                            & {r_a_1}                               & \textbf{{{s_a_1}}}                     & {r_a_2}                              & \textbf{{{s_a_2}}}                    \\
{comp_b}                       & {r_b_0}                                       & \textbf{{{s_b_0}}}                           & {r_b_1}                               & \textbf{{{s_b_1}}}                     & {r_b_2}                              & \textbf{{{s_b_2}}}                    \\ \arrayrulecolor{{light_grey}}\hline
	"""
        return out.format(
            fund_name=fund_name,
            prev_pd_return=prev_pd_return,
            series_abbrev=series_abbrev,
            r_this_1=r_this_1,
            r_this_2=r_this_2,
            comp_a=comp_a,
            comp_b=comp_b,
            r_a_0=form_as_percent(r_a[0], 2),
            r_a_1=r_a[1],  # already percent
            r_a_2=r_a[2],
            r_b_0=form_as_percent(r_b[0], 2),
            r_b_1=r_b[1],
            r_b_2=r_b[2],
            s_a_0=s_a_0,
            s_a_1=s_a_1,
            s_a_2=s_a_2,
            s_b_0=s_b_0,
            s_b_1=s_b_1,
            s_b_2=s_b_2,
        )
    else:
        out = r"""
	\textbf{{Lucid {fund_name} - Series {series_abbrev}}}                    & \textbf{{{prev_pd_return}}}                              & \textbf{{-}}                                  & \textbf{{{r_this_1}}}                               & \textbf{{-}}                           & \textbf{{{r_this_2}}}                             & \textbf{{-}}                          \\
{comp_a}                       & {r_a_0}                                       & \textbf{{{s_a_0}}}                            & {r_a_1}                               & \textbf{{{s_a_1}}}                     & {r_a_2}                              & \textbf{{{s_a_2}}}                    \\
{comp_b}                       & {r_b_0}                                       & \textbf{{{s_b_0}}}                           & {r_b_1}                               & \textbf{{{s_b_1}}}                     & {r_b_2}                              & \textbf{{{s_b_2}}}                    \\
{comp_c}                       & {r_c_0}                                       & \textbf{{{s_c_0}}}                            & {r_c_1}                               & \textbf{{{s_c_1}}}                     & {r_c_2}                              & \textbf{{{s_c_2}}}                    \\ \arrayrulecolor{{light_grey}}\hline
	"""
        return out.format(
            fund_name=fund_name,
            prev_pd_return=prev_pd_return,
            series_abbrev=series_abbrev,
            r_this_1=r_this_1,
            r_this_2=r_this_2,
            comp_a=comp_a,
            comp_b=comp_b,
            comp_c=comp_c,
            r_a_0=form_as_percent(r_a[0], 2),
            r_a_1=r_a[1],  # already percent
            r_a_2=r_a[2],
            r_b_0=form_as_percent(r_b[0], 2),
            r_b_1=r_b[1],
            r_b_2=r_b[2],
            r_c_0=form_as_percent(r_c[0], 2),
            r_c_1=r_c[1],
            r_c_2=r_c[2],
            s_a_0=s_a_0,
            s_a_1=s_a_1,
            s_a_2=s_a_2,
            s_b_0=s_b_0,
            s_b_1=s_b_1,
            s_b_2=s_b_2,
            s_c_0=s_c_0,
            s_c_1=s_c_1,
            s_c_2=s_c_2,
        )


def addl_coll_breakdown(alloc_aa_a, oc_aa_a, alloc_bbb, oc_bbb, alloc_bb, oc_bb):
    if alloc_aa_a == "n/a":
        return ""
    else:
        outp = r"""IG Repo: AA to A & {alloc_aa_a} & {oc_aa_a} \\
		IG Repo: BBB & {alloc_bbb} & {oc_bbb} \\
		Repo: BB & {alloc_bb} & {oc_bb} \\"""
        return outp.format(
            alloc_aa_a=alloc_aa_a,
            oc_aa_a=oc_aa_a,
            alloc_bbb=alloc_bbb,
            oc_bbb=oc_bbb,
            alloc_bb=alloc_bb,
            oc_bb=oc_bb,
        )


def colltable(
    inclextra,
    secured_by,
    series_abbrev,
    include_aaa_in_usg_bucket,
    alloc_aaa,
    alloc_aa_a,
    alloc_bbb,
    alloc_bb,
    alloc_tbills,
    alloc_total,
    oc_total,
    oc_tbills,
    oc_bb,
    oc_bbb,
    oc_aa_a,
    oc_aaa,
):
    if inclextra:
        out = r"""
		\renewcommand{{\arraystretch}}{{{vstretch}}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
		>{{\columncolor[HTML]{{EFEFEF}}}}p{{3.5cm}} 
		>{{\columncolor[HTML]{{EFEFEF}}}}c
		>{{\columncolor[HTML]{{EFEFEF}}}}c!{{\color{{light_grey}}\vrule}}}}
		\arrayrulecolor{{light_grey}}\hline
		\multicolumn{{3}}{{!{{\color{{light_grey}}\vrule}}l!{{\color{{light_grey}}\vrule}}}}{{\rowcolor{{lucid_blue}}{{\color[HTML]{{FFFFFF}}\textbf{{Series Collateral Overview\textsuperscript{{4}}}}}}}} \\
		\multicolumn{{3}}{{!{{\color{{light_grey}}\vrule}}p{{8.2cm}}!{{\color{{light_grey}}\vrule}}}}{{\rowcolor[HTML]{{EFEFEF}}{{\textbf{{Series {series_abbrev}}}: Secured by \textbf{{{secured_by}}}, with daily valuations \& margining.}}}} \\
		& & \\
		 & \textbf{{\% Portfolio}} & \textbf{{O/C Rate}}\\
		{usg_aaa_cat} & {alloc_aaa} & {oc_aaa} \\
		IG Repo: AA to A & {alloc_aa_a} & {oc_aa_a} \\
		IG Repo: BBB & {alloc_bbb} & {oc_bbb} \\
		Repo: BB & {alloc_bb} & {oc_bb} \\
		T-Bills; Gov't MMF & {alloc_tbills} & {oc_tbills} \\ \cline{{2-2}} \cline{{3-3}} 
		\textbf{{Total}} & {alloc_total} & \textbf{{{oc_total}}} \\\arrayrulecolor{{light_grey}}\hline
		\end{{tabular}}
		"""
        return out.format(
            vstretch=1.48,
            secured_by=secured_by,
            series_abbrev=series_abbrev,
            usg_aaa_cat=(
                "US Govt/AAA Repo" if include_aaa_in_usg_bucket else "US Govt Repo"
            ),
            alloc_aaa=alloc_aaa,
            alloc_aa_a=alloc_aa_a,
            alloc_bbb=alloc_bbb,
            alloc_bb=alloc_bb,
            alloc_tbills=alloc_tbills,
            alloc_total=alloc_total,
            oc_total=oc_total,
            oc_tbills=oc_tbills,
            oc_bb=oc_bb,
            oc_bbb=oc_bbb,
            oc_aa_a=oc_aa_a,
            oc_aaa=oc_aaa,
        )
    else:
        out = r"""
		\renewcommand{{\arraystretch}}{{{vstretch}}}\begin{{tabular}}{{!{{\color{{light_grey}}\vrule}}
		>{{\columncolor[HTML]{{EFEFEF}}}}p{{3.5cm}} 
		>{{\columncolor[HTML]{{EFEFEF}}}}c
		>{{\columncolor[HTML]{{EFEFEF}}}}c!{{\color{{light_grey}}\vrule}}}}
		\arrayrulecolor{{light_grey}}\hline
		\multicolumn{{3}}{{!{{\color{{light_grey}}\vrule}}l!{{\color{{light_grey}}\vrule}}}}{{\rowcolor{{lucid_blue}}{{\color[HTML]{{FFFFFF}}\textbf{{Series Collateral Overview\textsuperscript{{4}}}}}}}} \\
		\multicolumn{{3}}{{!{{\color{{light_grey}}\vrule}}p{{8.2cm}}!{{\color{{light_grey}}\vrule}}}}{{\rowcolor[HTML]{{EFEFEF}}{{\textbf{{Series {series_abbrev}}}: Secured by \textbf{{{secured_by}}}, with daily valuations \& margining.}}}} \\
		& & \\
		 & \textbf{{\% Portfolio}} & \textbf{{O/C Rate}}\\
		{usg_aaa_cat} & {alloc_aaa} & {oc_aaa} \\
		T-Bills; Gov't MMF & {alloc_tbills} & {oc_tbills} \\ \cline{{2-2}} \cline{{3-3}} 
		\textbf{{Total}} & {alloc_total} & \textbf{{{oc_total}}} \\\arrayrulecolor{{light_grey}}\hline
		\end{{tabular}}
		"""
        return out.format(
            vstretch=1.91,
            secured_by=secured_by,
            series_abbrev=series_abbrev,
            usg_aaa_cat=(
                "US Govt/AAA Repo" if include_aaa_in_usg_bucket else "US Govt Repo"
            ),
            alloc_aaa=alloc_aaa,
            # alloc_cp=alloc_cp,
            alloc_tbills=alloc_tbills,
            alloc_total=alloc_total,
            oc_total=oc_total,
            oc_tbills=oc_tbills,
            # oc_cp=oc_cp,
            oc_aaa=oc_aaa,
        )


def plotify_new(x_col, y_col):
    # output: (x, y) for (x in x_col, y in y_col)
    # x_col:
    return


def plotify(ws, x_col, y_col, start, end):
    outp = ""
    for row in range(start, end + 1):
        if ws[x_col + str(row)].value and ws[y_col + str(row)].value:
            outp = (
                outp
                + "("
                + ws[x_col + str(row)].value.strftime("%Y-%m-%d")
                + ","
                + str(round(100 * ws[y_col + str(row)].value, 3))
                + ") "
            )
    return outp


def snapshot_graph(
    hspace,
    graphwidth,
    graphheight,
    maxreturn,
    series_abbrev,
    comp_a,
    comp_b,
    comp_c,
    this_r,
    ra,
    rb,
    rc,
):
    if comp_c is not None:  # when we have 3 comparables - for all prime series
        out = r"""
		  \hspace*{{{hspace}cm}}\resizebox {{{graphwidth}}} {{{graphheight}}} {{\begin{{tikzpicture}}
	\begin{{axis}}[
		title style = {{font = \small}},
		axis line style = {{light_grey}},
		title={{{{Performance vs Benchmarks}}}},
		ymin=3, ymax={maxreturn}, %MAXRETURN HERE
	   symbolic x coords={{Series {series_abbrev},{comp_a},{comp_b},{comp_c}}},
		xtick={{Series {series_abbrev},{comp_a},{comp_b},{comp_c}}},
		x tick label style={{anchor=north,font=\scriptsize,/pgf/number format/assume math mode}},
		yticklabel=\pgfmathparse{{\tick}}\pgfmathprintnumber{{\pgfmathresult}}\,\%,
		y tick label style = {{/pgf/number format/.cd,
				fixed,
				fixed zerofill,
				precision=2,
				/pgf/number format/assume math mode
		}},
		ytick distance={tickdist},
		bar width = 8mm, x = 2.46cm,
		xtick pos=bottom,ytick pos=left,
		every node near coord/.append style={{font=\fontsize{{8}}{{8}}\selectfont,/pgf/number format/.cd,
				fixed,
				fixed zerofill,
				precision=2,/pgf/number format/assume math mode}},
		]
	\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			(Series {series_abbrev},{this_r}) 
		}};
	\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			({comp_a},{ra}) 
		}};
	\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			({comp_b},{rb}) 
		}};
	\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			({comp_c},{rc}) 
		}};
	\end{{axis}}
		\end{{tikzpicture}}}}

		"""
        return out.format(
            hspace=hspace,
            tickdist=0.5 if max(this_r, ra, rb, rc) + 0.1 >= 1 else 0.25,
            graphwidth=graphwidth,
            graphheight=graphheight,
            maxreturn=max(this_r, ra, rb, rc) + 1,
            series_abbrev=series_abbrev,
            comp_a=benchmark_shorten(comp_a) if "CRANE" in comp_a.upper() else comp_a,
            comp_b=benchmark_shorten(comp_b) if "CRANE" in comp_b.upper() else comp_b,
            comp_c=benchmark_shorten(comp_c) if "CRANE" in comp_c.upper() else comp_c,
            this_r=this_r,
            ra=ra,
            rb=rb,
            rc=rc,
        )
    else:  # when we only have 2 comparables - for all usg
        out = r"""
		  \hspace*{{{hspace}cm}}\resizebox {{{graphwidth}}} {{{graphheight}}} {{\begin{{tikzpicture}}
	\begin{{axis}}[
		title style = {{font = \small}},
		axis line style = {{light_grey}},
			title={{{{Performance vs Benchmarks}}}},
		ymin=2, ymax={maxreturn}, %MAXRETURN HERE
	   symbolic x coords={{Series {series_abbrev},{comp_a},{comp_b}}},
		xtick={{Series {series_abbrev},{comp_a},{comp_b}}},
		x tick label style={{anchor=north,font=\scriptsize,/pgf/number format/assume math mode}},
		yticklabel=\pgfmathparse{{\tick}}\pgfmathprintnumber{{\pgfmathresult}}\,\%,
		y tick label style = {{/pgf/number format/.cd,
				fixed,
				fixed zerofill,
				precision=2,
				/pgf/number format/assume math mode
		}},
		ytick distance={tickdist},
		bar width = 10mm, x = 3.7cm,
		xtick pos=bottom,ytick pos=left,
		every node near coord/.append style={{font=\fontsize{{8}}{{8}}\selectfont,/pgf/number format/.cd,
				fixed,
				fixed zerofill,
				precision=2,/pgf/number format/assume math mode}},
		]
	\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			(Series {series_abbrev},{this_r}) 
		}};
	\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			({comp_a},{ra}) 
		}};
	\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={{shadow yshift=-1pt, shadow xshift=1pt}}] 
		coordinates {{
			({comp_b},{rb}) 
		}};
	\end{{axis}}
		\end{{tikzpicture}}}}
	
		"""
        return out.format(
            hspace=hspace,
            tickdist=0.5 if maxreturn >= 1 else 0.25,
            graphwidth=graphwidth,
            graphheight=graphheight,
            maxreturn=max(this_r, ra, rb, rc) + 1,
            series_abbrev=series_abbrev,
            comp_a=benchmark_shorten(comp_a) if "CRANE" in comp_a.upper() else comp_a,
            comp_b=benchmark_shorten(comp_b) if "CRANE" in comp_b.upper() else comp_b,
            this_r=this_r,
            ra=ra,
            rb=rb,
        )


def performance_graph(
    titleincl,
    graphhspace,
    graphwidth,
    graphheight,
    fund_name,
    zero_date,
    min_return,
    max_return,
    graphx,
    graphbarwidth,
    return_plot,
    comp_a_plot,
    comp_b_plot,
    series_abbrev,
    comp_a,
    comp_b,
):
    if fund_name.upper() == "USG":
        out = performance_graph_template_usg
    else:
        out = performance_graph_template_other
    # Terri graph
    return out.format(
        graphhspace=graphhspace,
        graphwidth=graphwidth,
        graphheight=graphheight,
        fund_name=fund_name,
        title=(
            r"""
		title={{Performance vs Benchmark}},"""
            if titleincl
            else ""
        ),
        zero_date=zero_date,
        min_return=min_return,
        max_return=max_return,
        graphx=graphx,
        graphbarwidth=graphbarwidth,
        return_plot=return_plot,
        comp_a_plot=comp_a_plot,
        comp_b_plot=comp_b_plot,
        series_abbrev=series_abbrev,
        comp_a=comp_a,
        comp_b=comp_b,
    )
