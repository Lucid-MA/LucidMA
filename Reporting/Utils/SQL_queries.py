OC_query = f"""
select
case when tradepieces.company = 44 then 'USG' when tradepieces.company = 45 then 'Prime' end fund,
RTRIM(Tradepieces.LEDGERNAME) as "Series",
Tradepieces.TRADEPIECE as "Trade ID",
RTRIM(TRADETYPES.DESCRIPTION) as "TradeType",
Tradepieces.STARTDATE as "Start Date",
CASE WHEN Tradepieces.CLOSEDATE is null then tradepieces.enddate else Tradepieces.CLOSEDATE 
END as "End Date",
Tradepieces.FX_MONEY as "Money",
ltrim(RTRIM(Tradepieces.CONTRANAME)) as "Counterparty",
coalesce(tradepiececalcdatas.lastrate, Tradepieces.REPORATE) as "Orig. Rate",
Tradepieces.PRICE as "Orig. Price",
ltrim(rtrim(Tradepieces.ISIN)) as "BondID",
Tradepieces.PAR * case when tradepieces.tradetype in (0, 22) then -1 else 1 end as "Par/Quantity",
case when RTRIM(TRADETYPES.DESCRIPTION) in ('ReverseFree','RepoFree') then 0 else Tradepieces.HAIRCUT end as "HairCut",
Tradecommissionpieceinfo.commissionvalue * 100 Spread,
ltrim(RTRIM(Tradepieces.acct_number)) 'cp short',
case when tradepieces.cusip = 'CASHUSD01' then 'USG' when tradepieces.tradepiece in (60320,60321,60258) then 'BBB' when tradepieces.comments = '' then ratings_tbl.rating else tradepieces.comments end as "Comments",
Tradepieces.FX_MONEY + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD "End Money",
case when rtrim(ISSUESUBTYPES3.DESCRIPTION) = 'CLO CRE' then 'CMBS' else RTRIM(CASE WHEN Tradepieces.cusip='CASHUSD01' THEN 'USD Cash'
ELSE ISSUESUBTYPES2.DESCRIPTION
END) end "Product Type",
RTRIM(CASE WHEN Tradepieces.cusip='CASHUSD01' THEN 'Cash'
ELSE ISSUESUBTYPES3.DESCRIPTION 
END) "Collateral Type"
from tradepieces 
INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE=TRADEPIECES.TRADEPIECE
INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE=TRADEPIECES.TRADEPIECE
INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE=TRADEPIECES.SHELLTRADETYPE
INNER JOIN ISSUES ON ISSUES.CUSIP=TRADEPIECEs.CUSIP
INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY=TRADEPIECES.CURRENCY_MONEY
INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL=TRADEPIECES.STATUSDETAIL
INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN=TRADEPIECES.STATUSMAIN
INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY=TRADEPIECES.ISSUECATEGORY
INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1=ISSUECATEGORIES.ISSUESUBTYPE1
INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2=ISSUECATEGORIES.ISSUESUBTYPE2
INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3=ISSUECATEGORIES.ISSUESUBTYPE3
INNER JOIN depositorys ON tradepieces.DEPOSITORYID = Depositorys.DEPOSITORYID
left join (
select distinct history_tradepieces.tradepiece, history_tradepieces.comments rating from history_tradepieces inner join (
select max(datetimeid) datetimeid, tradepiece from history_tradepieces inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece group by cast(datetimeid as date), tradepiece) maxtbl
on history_tradepieces.datetimeid = maxtbl.datetimeid and history_tradepieces.tradepiece = maxtbl.tradepiece
inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece
where cast(history_tradepieces.datetimeid as date) = cast(history_tradepieces.bookdate as date)
) ratings_tbl on ratings_tbl.tradepiece = tradepieces.tradepiece
where tradepieces.statusmain <> 6
and tradepieces.company in (44,45)
and (tradetypes.description = 'Reverse' or tradetypes.description = 'ReverseFree' or tradetypes.description = 'RepoFree')
order by tradepieces.company asc, tradepieces.ledgername asc, tradepieces.contraname asc
"""

all_securities_query = """
        SELECT DISTINCT CUSIP
        FROM ISSUES
        WHERE ltrim(rtrim(CUSIP)) not in 
        ('HEXZETA01','HEXZT----','HZLNT----','MCHY-----','MNTNCHRY1','OLIVEEUR-','OLIVEUSD-','OPPOR----','OPPORTUN1','PAAPLEUR-','PAAPLUSD-','PFIR-----','SSPRUCE--','STAPL----','STHAPPLE1','TREATY---','TREATYUS1','ALM2EUR--','ALM2USD--','ALMNDUSD1','ALMONDEUR','ALMONDUSD','ECYP-----','EELM-----','EWILLEUR-','EWILLUSD-');
        """

daily_price_securities_helix_query = """
            select distinct
            case when tradepieces.company = 44 then 'USG Fund' when tradepieces.company = 45 then 'Prime Fund' when tradepieces.COMPANY = 46 then 'MMT IM Fund' when tradepieces.COMPANY = 48 then 'LMCP Inv Fund'  when tradepieces.COMPANY = 49 then 'LucidRepo' end Fund,
            ltrim(rtrim(Tradepieces.ISIN)) BondID
            from tradepieces 
            where (tradepieces.isvisible = 1 or tradepieces.company = 49)
            and tradepieces.company in (44,45,46,48,49)
	        and ltrim(rtrim(Tradepieces.ISIN)) not in ('HEXZETA01','HEXZT----','HZLNT----','MCHY-----','MNTNCHRY1','OLIVEEUR-','OLIVEUSD-','OPPOR----','OPPORTUN1','PAAPLEUR-','PAAPLUSD-','PFIR-----','SSPRUCE--','STAPL----','STHAPPLE1','TREATY---','TREATYUS1','ALM2EUR--','ALM2USD--','ALMNDUSD1','ALMONDEUR','ALMONDUSD','ECYP-----','EELM-----','EWILLEUR-','EWILLUSD-')
            order by Fund ASC
        """

# Query trade with trade_type in (22,23) excluding (37090, 37089, 37088, 37087, 37086, 37085, 37084, 37083, 37082, 37081)
trade_helix_query = """
        DECLARE @valdate DATE;
        SET @valdate = ?; -- Replace with the desired date
        
        SELECT
            CONCAT(
                (CASE
                    WHEN tradepieces.company IN (44, 46) THEN tradepieces.tradepiece
                    WHEN LTRIM(RTRIM(tradepieces.ledgername)) = 'Master' AND tradepieces.company = 45 THEN Tradepieces.TRADEPIECE
                    ELSE COALESCE(
                        CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                        tradepiecexrefs.frontofficeid
                    )
                END),
                ' ',
                (CASE WHEN tradepieces.startdate = @valdate THEN 'TRANSMITTED' ELSE 'CLOSED' END)
            ) AS action_id,
            CASE
                WHEN tradepieces.company = 44 THEN 'USG'
                WHEN tradepieces.company = 45 THEN 'PRIME'
                WHEN tradepieces.company = 46 THEN 'MMT'
            END AS fund,
            UPPER(LTRIM(RTRIM(ledgername))) AS series,
            /* crucial column. if only one series in fund, this should be true, else false */
            CASE WHEN tradepieces.company <> 45 THEN 1 ELSE 0 END AS is_also_master,
            CASE
                WHEN COALESCE(
                    CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                    tradepiecexrefs.frontofficeid
                ) <> 0 THEN tradepieces.par * 1.0 / masterpieces.masterpar
                ELSE 1
            END AS used_alloc,
            tradepieces.tradetype AS trade_type,
            tradepieces.startdate AS start_date,
            CASE WHEN tradepieces.closedate IS NULL THEN tradepieces.enddate ELSE tradepieces.closedate END AS end_date,
            CASE WHEN tradepieces.enddate = @valdate THEN 1 ELSE 0 END AS set_to_term_on_date,
            tradepieces.cusip AS security,
            tradepieces.isgscc AS is_buy_sell,
            tradepieces.par AS quantity,
            tradepieces.money,
            (Tradepieces.money + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD) AS end_money,
            CASE
                WHEN (tradepieces.company = 45 AND LTRIM(RTRIM(tradepieces.ledgername)) = 'Master') OR tradepieces.company IN (44, 46)
                THEN COALESCE(
                    CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                    tradepiecexrefs.frontofficeid
                )
                ELSE ''
            END AS roll_of,
            CASE WHEN LTRIM(RTRIM(Tradepieces.acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(Tradepieces.acct_number)) END AS counterparty,
            Tradepieces.depository
        FROM
            tradepieces
            JOIN TRADEPIECECALCDATAS ON tradepieces.tradepiece = TRADEPIECECALCDATAS.tradepiece
            JOIN TRADECOMMISSIONPIECEINFO ON tradepieces.tradepiece = TRADECOMMISSIONPIECEINFO.TRADEPIECE
            JOIN TRADEPIECEXREFS ON TRADEPIECES.TRADEPIECE = TRADEPIECEXREFS.TRADEPIECE
            LEFT JOIN (
                SELECT tradepiece AS masterpiece, par AS masterpar
                FROM tradepieces
            ) AS masterpieces ON COALESCE(
                CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                tradepiecexrefs.frontofficeid
            ) = masterpieces.masterpiece WHERE
            (Tradepieces.startdate = @valdate OR CASE WHEN tradepieces.closedate IS NULL THEN tradepieces.enddate ELSE tradepieces.closedate END = @valdate)
            AND tradepieces.company IN (44, 45)
            AND tradepieces.statusmain NOT IN (6)
            AND Tradepieces.tradetype IN (0, 1)
            AND Tradepieces.tradepiece NOT IN (37090, 37089, 37088, 37087, 37086, 37085, 37084, 37083, 37082, 37081)
            ORDER BY
            tradepieces.company,
            action_id,
            CASE WHEN UPPER(LTRIM(RTRIM(tradepieces.ledgername))) = 'MASTER' THEN 0 ELSE 1 END;
        """

# result_df = execute_sql_query(trade_helix_query, "sql_server_1", params=[(valdate,)])

# Query trade with trade_type in (0,1)
trade_free_helix_query = """
        DECLARE @valdate DATE;
        SET @valdate = ?; -- Replace with the desired date
        
        SELECT
            CONCAT(
                (CASE
                    WHEN tradepieces.company IN (44, 46) THEN tradepieces.tradepiece
                    WHEN LTRIM(RTRIM(tradepieces.ledgername)) = 'Master' AND tradepieces.company = 45 THEN Tradepieces.TRADEPIECE
                    ELSE COALESCE(
                        CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                        tradepiecexrefs.frontofficeid
                    )
                END),
                ' ',
                (CASE WHEN tradepieces.startdate = @valdate THEN 'TRANSMITTED' ELSE 'CLOSED' END)
            ) AS action_id,
            CASE
                WHEN tradepieces.company = 44 THEN 'USG'
                WHEN tradepieces.company = 45 THEN 'PRIME'
                WHEN tradepieces.company = 46 THEN 'MMT'
            END AS fund,
            UPPER(LTRIM(RTRIM(ledgername))) AS series,
            CASE
                WHEN COALESCE(
                    CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                    tradepiecexrefs.frontofficeid
                ) <> 0 THEN tradepieces.par * 1.0 / masterpieces.masterpar
                ELSE 1
            END AS used_alloc,
            /* crucial column. if only one series in fund, this should be true, else false */
            CASE WHEN tradepieces.company <> 45 THEN 1 ELSE 0 END AS is_also_master,
            tradepieces.startdate AS start_date,
            tradepieces.closedate AS close_date,
            tradepieces.enddate AS end_date,
            par * CASE
                WHEN (tradepieces.tradetype = 23 AND tradepieces.startdate = @valdate) OR (tradepieces.tradetype = 22 AND (tradepieces.CLOSEDATE = @valdate OR tradepieces.enddate = @valdate)) THEN 1
                WHEN (tradepieces.tradetype = 22 AND tradepieces.startdate = @valdate) OR (tradepieces.tradetype = 23 AND (tradepieces.CLOSEDATE = @valdate OR tradepieces.enddate = @valdate)) THEN -1
                ELSE 0
            END AS "amount",
            tradepieces.tradetype AS trade_type,
            tradepieces.cusip AS "security",
            CASE WHEN LTRIM(RTRIM(Tradepieces.acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(Tradepieces.acct_number)) END AS "counterparty",
            CONCAT(
                CASE
                    WHEN (tradepieces.tradetype = 23 AND tradepieces.startdate = @valdate) THEN 'Receive '
                    WHEN (tradepieces.tradetype = 22 AND tradepieces.startdate = @valdate) THEN 'Pay '
                    WHEN (tradepieces.tradetype = 23 AND (tradepieces.CLOSEDATE = @valdate OR tradepieces.enddate = @valdate)) THEN 'Return '
                    WHEN (tradepieces.tradetype = 22 AND (tradepieces.CLOSEDATE = @valdate OR tradepieces.enddate = @valdate)) THEN 'Receive returned '
                END,
                CASE WHEN LTRIM(RTRIM(Tradepieces.acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(Tradepieces.acct_number)) END,
                ' margin'
            ) AS "description"
        FROM
            tradepieces
            JOIN TRADEPIECECALCDATAS ON tradepieces.tradepiece = TRADEPIECECALCDATAS.tradepiece
            JOIN TRADECOMMISSIONPIECEINFO ON tradepieces.tradepiece = TRADECOMMISSIONPIECEINFO.TRADEPIECE
            JOIN TRADEPIECEXREFS ON TRADEPIECES.TRADEPIECE = TRADEPIECEXREFS.TRADEPIECE
            LEFT JOIN (
                SELECT tradepiece AS masterpiece, par AS masterpar
                FROM tradepieces
            ) AS masterpieces ON COALESCE(
                CASE WHEN TRADECOMMISSIONPIECEINFO.commissionvalue2 = 0 THEN NULL ELSE TRADECOMMISSIONPIECEINFO.commissionvalue2 END,
                tradepiecexrefs.frontofficeid
            ) = masterpieces.masterpiece
        WHERE
            (Tradepieces.startdate = @valdate OR Tradepieces.enddate = @valdate OR Tradepieces.closedate = @valdate)
            AND tradepieces.company IN (44, 45)
            AND Tradepieces.tradetype IN (22, 23)
            AND tradepieces.statusmain NOT IN (6)
        ORDER BY
            tradepieces.company,
            CASE WHEN UPPER(LTRIM(RTRIM(tradepieces.ledgername))) = 'MASTER' THEN 0 ELSE 1 END;
        """

net_cash_by_counterparty_helix_query = """
        DECLARE @valdate DATE;
        SET @valdate = ?;
        
        SELECT
            tbl1.fund,
            CASE WHEN LTRIM(RTRIM(TBL1.acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(TBL1.acct_number)) END AS acct_number,
            LTRIM(RTRIM(tbl1.ledgername)) AS ledgername,
            tbl1.net_cash,
            CASE WHEN tbl2.activity IS NULL THEN 0 ELSE tbl2.activity END AS activity,
            tbl1.is_also_master
        FROM
            (SELECT
                CASE
                    WHEN company = 44 THEN 'USG'
                    WHEN company = 45 THEN 'PRIME'
                    WHEN tradepieces.company = 46 THEN 'MMT'
                    ELSE 'Other'
                END AS fund,
                CASE WHEN LTRIM(RTRIM(acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(acct_number)) END AS acct_number,
                LTRIM(RTRIM(ledgername)) AS ledgername,
                ROUND(SUM(
                    CASE WHEN tradetype = 22 THEN -1 ELSE 1 END *
                    CASE WHEN (tradepieces.closedate = @valdate OR tradepieces.enddate = @valdate) THEN 0 ELSE 1 END *
                    par
                ), 2) AS 'net_cash',
                CASE WHEN company <> 45 THEN 1 ELSE 0 END AS is_also_master
            FROM
                tradepieces
            WHERE
                (tradepieces.startdate <= @valdate AND (tradepieces.closedate >= @valdate OR ((tradepieces.enddate IS NULL OR tradepieces.enddate >= @valdate) AND tradepieces.closedate IS NULL))) AND
                company IN (44, 45) AND
                tradetype IN (22, 23) AND
                cusip = 'CASHUSD01' AND
                statusmain NOT IN (6)
            GROUP BY
                company,
                ledgername,
                CASE WHEN LTRIM(RTRIM(acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(acct_number)) END
            ) tbl1
            FULL OUTER JOIN
            (SELECT
                CASE
                    WHEN company = 44 THEN 'USG'
                    WHEN company = 45 THEN 'PRIME'
                    WHEN tradepieces.company = 46 THEN 'MMT'
                    ELSE 'Other'
                END AS fund,
                CASE WHEN LTRIM(RTRIM(acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(acct_number)) END AS acct_number,
                LTRIM(RTRIM(ledgername)) AS ledgername,
                ROUND(SUM(
                    CASE WHEN tradetype = 22 THEN -1 ELSE 1 END *
                    CASE WHEN startdate = @valdate THEN 1 ELSE -1 END *
                    par
                ), 2) AS 'activity',
                CASE WHEN company <> 45 THEN 1 ELSE 0 END AS is_also_master
            FROM
                tradepieces
            WHERE
                (tradepieces.startdate = @valdate OR tradepieces.closedate = @valdate OR (tradepieces.enddate = @valdate AND tradepieces.closedate IS NULL)) AND
                company IN (44, 45) AND
                tradetype IN (22, 23) AND
                cusip = 'CASHUSD01' AND
                statusmain NOT IN (6)
            GROUP BY
                company,
                LTRIM(RTRIM(ledgername)),
                CASE WHEN LTRIM(RTRIM(acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(acct_number)) END
            ) tbl2 ON
            tbl1.fund = tbl2.fund AND
            CASE WHEN LTRIM(RTRIM(TBL1.acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(TBL1.acct_number)) END =
            CASE WHEN LTRIM(RTRIM(TBL2.acct_number)) = '400CAPTX' THEN 'TEX' ELSE LTRIM(RTRIM(TBL2.acct_number)) END AND
            LTRIM(RTRIM(tbl1.ledgername)) = LTRIM(RTRIM(tbl2.ledgername))
        ORDER BY
            tbl1.fund,
            CASE WHEN UPPER(LTRIM(RTRIM(tbl1.ledgername))) = 'MASTER' THEN 0 ELSE 1 END;
        """

# Yating's original query on all trade
daily_report_helix_trade_query_original = """
DECLARE @valdate AS DATE
SET @valdate = ?

USE HELIXREPO_PROD_02

IF OBJECT_ID('tempdb..#tradedata') IS NOT NULL DROP TABLE #tradedata

SELECT
    tradepieces.company,
    Tradepieces.LEDGERNAME AS "Series",
    Tradepieces.TRADEPIECE AS "Trade ID",
    RTRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
    tradepieces.TRADEDATE AS "Trade Date",
    Tradepieces.STARTDATE AS "Start Date",
    Tradepieces.CLOSEDATE AS "Close Date",
    tradepieces.enddate AS "End Date",
    Tradepieces.FX_MONEY AS "Money",
    Tradepieces.CONTRANAME AS "Counterparty",
    Tradepieces.REPORATE AS "Orig. Rate",
    Tradepieces.PRICE AS "Orig. Price",
    tradepiececalcdatas.CURRENTPRICE AS "Current Price",
    tradepiececalcdatas.CURRENTMBSFACTOR AS "Current Factor",
    LTRIM(RTRIM(Tradepieces.ISIN)) AS "BondID",
    Tradepieces.statusmain AS "Status",
    tradepiecexrefs.frontofficeid AS "Alloc Of",
    Tradepieces.PAR * CASE WHEN tradepieces.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(TRADETYPES.DESCRIPTION) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE Tradepieces.HAIRCUT END AS "HairCut",
    Tradecommissionpieceinfo.commissionvalue * 100 AS "Spread",
    Tradepieces.PAR * tradepiececalcdatas.CURRENTPRICE * tradepiececalcdatas.CURRENTMBSFACTOR / 100 AS "Market Value",
    Tradepieces.ACCT_NUMBER AS "CP Short",
    tradepieces.comments AS "Comments",
    Tradepieces.FX_MONEY + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "End Money",
    CASE
        WHEN RTRIM(ISSUESUBTYPES3.DESCRIPTION) = 'CLO CRE' THEN 'CMBS'
        ELSE RTRIM(CASE WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
                        ELSE ISSUESUBTYPES2.DESCRIPTION
                   END)
    END AS "Product Type",
    RTRIM(CASE WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
               ELSE ISSUESUBTYPES3.DESCRIPTION
          END) AS "Collateral Type"
INTO #tradedata
FROM tradepieces
INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_MONEY
INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
INNER JOIN TRADEPIECEXREFS ON tradepieces.tradepiece = TRADEPIECEXREFS.TRADEPIECE
LEFT JOIN (
    SELECT DISTINCT history_tradepieces.tradepiece, history_tradepieces.comments AS rating
    FROM history_tradepieces
    INNER JOIN (
        SELECT MAX(datetimeid) AS datetimeid, tradepiece
        FROM history_tradepieces
        INNER JOIN (
            SELECT tradepiece AS tid
            FROM tradepieces
            WHERE isvisible = 1
        ) AS vistbl
        ON vistbl.tid = history_tradepieces.tradepiece
        GROUP BY CAST(datetimeid AS DATE), tradepiece
    ) AS maxtbl
    ON history_tradepieces.datetimeid = maxtbl.datetimeid
    AND history_tradepieces.tradepiece = maxtbl.tradepiece
    INNER JOIN (
        SELECT tradepiece AS tid
        FROM tradepieces
        WHERE isvisible = 1
    ) AS vistbl
    ON vistbl.tid = history_tradepieces.tradepiece
    WHERE CAST(history_tradepieces.datetimeid AS DATE) = CAST(history_tradepieces.bookdate AS DATE)
) AS ratings_tbl
ON ratings_tbl.tradepiece = tradepieces.tradepiece
WHERE tradepieces.enddate = @valdate
OR tradepieces.closedate = @valdate
OR tradepieces.startdate = @valdate
ORDER BY tradepieces.company ASC, tradepieces.ledgername ASC, tradepieces.contraname ASC

SELECT *
FROM #tradedata
ORDER BY [Start Date]
"""

# Use for reporting
current_trade_daily_report_helix_trade_query = """
DECLARE @valdate AS DATE
SET @valdate = ?

SELECT
    tradepieces.company,
    Tradepieces.LEDGERNAME AS "Series",
    Tradepieces.TRADEPIECE AS "Trade ID",
    RTRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
    tradepieces.TRADEDATE AS "Trade Date",
    Tradepieces.STARTDATE AS "Start Date",
    Tradepieces.CLOSEDATE AS "Close Date",
    tradepieces.enddate AS "End Date",
    Tradepieces.FX_MONEY AS "Money",
    Tradepieces.CONTRANAME AS "Counterparty",
    Tradepieces.REPORATE AS "Orig. Rate",
    Tradepieces.PRICE AS "Orig. Price",
    tradepiececalcdatas.CURRENTPRICE AS "Current Price",
    tradepiececalcdatas.CURRENTMBSFACTOR AS "Current Factor",
    LTRIM(RTRIM(Tradepieces.ISIN)) AS "BondID",
    Tradepieces.statusmain AS "Status",
    tradepiecexrefs.frontofficeid AS "Alloc Of",
    Tradepieces.PAR * CASE WHEN tradepieces.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(TRADETYPES.DESCRIPTION) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE Tradepieces.HAIRCUT END AS "HairCut",
    Tradecommissionpieceinfo.commissionvalue * 100 AS "Spread",
    Tradepieces.PAR * tradepiececalcdatas.CURRENTPRICE * tradepiececalcdatas.CURRENTMBSFACTOR / 100 AS "Market Value",
    Tradepieces.ACCT_NUMBER AS "CP Short",
    tradepieces.comments AS "Comments",
    Tradepieces.FX_MONEY + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "End Money",
    CASE
        WHEN RTRIM(ISSUESUBTYPES3.DESCRIPTION) = 'CLO CRE' THEN 'CMBS'
        ELSE RTRIM(CASE WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
                        ELSE ISSUESUBTYPES2.DESCRIPTION
                   END)
    END AS "Product Type",
    RTRIM(CASE WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
               ELSE ISSUESUBTYPES3.DESCRIPTION
          END) AS "Collateral Type",
    Tradepieces.USERNAME AS "User",
    Tradepieces.ISSUEDESCRIPTION AS "Issue Description",
    Tradepieces.ENTERDATETIMEID AS "Entry Time"
FROM tradepieces
INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_MONEY
INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
INNER JOIN TRADEPIECEXREFS ON tradepieces.tradepiece = TRADEPIECEXREFS.TRADEPIECE
LEFT JOIN (
    SELECT DISTINCT history_tradepieces.tradepiece, history_tradepieces.comments AS rating
    FROM history_tradepieces
    INNER JOIN (
        SELECT MAX(datetimeid) AS datetimeid, tradepiece
        FROM history_tradepieces
        INNER JOIN (
            SELECT tradepiece AS tid
            FROM tradepieces
            WHERE isvisible = 1
        ) AS vistbl
        ON vistbl.tid = history_tradepieces.tradepiece
        GROUP BY CAST(datetimeid AS DATE), tradepiece
    ) AS maxtbl
    ON history_tradepieces.datetimeid = maxtbl.datetimeid
    AND history_tradepieces.tradepiece = maxtbl.tradepiece
    INNER JOIN (
        SELECT tradepiece AS tid
        FROM tradepieces
        WHERE isvisible = 1
    ) AS vistbl
    ON vistbl.tid = history_tradepieces.tradepiece
    WHERE CAST(history_tradepieces.datetimeid AS DATE) = CAST(history_tradepieces.bookdate AS DATE)
) AS ratings_tbl
ON ratings_tbl.tradepiece = tradepieces.tradepiece
WHERE CAST(tradepieces.ENTERDATETIMEID AS DATE) = @valdate
AND CAST(tradepieces.STARTDATE AS DATE) >= @valdate
ORDER BY tradepieces.company ASC, tradepieces.ledgername ASC, tradepieces.contraname ASC, [Start Date]
"""


# Use for reporting
as_of_trade_daily_report_helix_trade_query = """
DECLARE @valdate AS DATE
SET @valdate = ?

SELECT
    tradepieces.company,
    Tradepieces.LEDGERNAME AS "Series",
    Tradepieces.TRADEPIECE AS "Trade ID",
    RTRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
    tradepieces.TRADEDATE AS "Trade Date",
    Tradepieces.STARTDATE AS "Start Date",
    Tradepieces.CLOSEDATE AS "Close Date",
    tradepieces.enddate AS "End Date",
    Tradepieces.FX_MONEY AS "Money",
    Tradepieces.CONTRANAME AS "Counterparty",
    Tradepieces.REPORATE AS "Orig. Rate",
    Tradepieces.PRICE AS "Orig. Price",
    tradepiececalcdatas.CURRENTPRICE AS "Current Price",
    tradepiececalcdatas.CURRENTMBSFACTOR AS "Current Factor",
    LTRIM(RTRIM(Tradepieces.ISIN)) AS "BondID",
    Tradepieces.statusmain AS "Status",
    tradepiecexrefs.frontofficeid AS "Alloc Of",
    Tradepieces.PAR * CASE WHEN tradepieces.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(TRADETYPES.DESCRIPTION) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE Tradepieces.HAIRCUT END AS "HairCut",
    Tradecommissionpieceinfo.commissionvalue * 100 AS "Spread",
    Tradepieces.PAR * tradepiececalcdatas.CURRENTPRICE * tradepiececalcdatas.CURRENTMBSFACTOR / 100 AS "Market Value",
    Tradepieces.ACCT_NUMBER AS "CP Short",
    tradepieces.comments AS "Comments",
    Tradepieces.FX_MONEY + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "End Money",
    CASE
        WHEN RTRIM(ISSUESUBTYPES3.DESCRIPTION) = 'CLO CRE' THEN 'CMBS'
        ELSE RTRIM(CASE WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
                        ELSE ISSUESUBTYPES2.DESCRIPTION
                   END)
    END AS "Product Type",
    RTRIM(CASE WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
               ELSE ISSUESUBTYPES3.DESCRIPTION
          END) AS "Collateral Type",
    Tradepieces.USERNAME AS "User",
    Tradepieces.ISSUEDESCRIPTION AS "Issue Description",
    Tradepieces.ENTERDATETIMEID AS "Entry Time"
FROM tradepieces
INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_MONEY
INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
INNER JOIN TRADEPIECEXREFS ON tradepieces.tradepiece = TRADEPIECEXREFS.TRADEPIECE
LEFT JOIN (
    SELECT DISTINCT history_tradepieces.tradepiece, history_tradepieces.comments AS rating
    FROM history_tradepieces
    INNER JOIN (
        SELECT MAX(datetimeid) AS datetimeid, tradepiece
        FROM history_tradepieces
        INNER JOIN (
            SELECT tradepiece AS tid
            FROM tradepieces
            WHERE isvisible = 1
        ) AS vistbl
        ON vistbl.tid = history_tradepieces.tradepiece
        GROUP BY CAST(datetimeid AS DATE), tradepiece
    ) AS maxtbl
    ON history_tradepieces.datetimeid = maxtbl.datetimeid
    AND history_tradepieces.tradepiece = maxtbl.tradepiece
    INNER JOIN (
        SELECT tradepiece AS tid
        FROM tradepieces
        WHERE isvisible = 1
    ) AS vistbl
    ON vistbl.tid = history_tradepieces.tradepiece
    WHERE CAST(history_tradepieces.datetimeid AS DATE) = CAST(history_tradepieces.bookdate AS DATE)
) AS ratings_tbl
ON ratings_tbl.tradepiece = tradepieces.tradepiece
WHERE CAST(tradepieces.ENTERDATETIMEID AS DATE) = @valdate
AND CAST(tradepieces.STARTDATE AS DATE) < @valdate
ORDER BY tradepieces.company ASC, tradepieces.ledgername ASC, tradepieces.contraname ASC, [Start Date]
"""