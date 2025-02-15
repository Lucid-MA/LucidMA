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


OC_query_historical_v2 = f"""
WITH active_trades AS (
    SELECT tradepiece
    FROM tradepieces
    WHERE startdate <= :valdate
    AND (closedate IS NULL OR closedate >= :valdate OR enddate >= :valdate)
),
latest_ratings AS (
    SELECT ht.tradepiece, ht.comments AS rating
    FROM history_tradepieces ht
    JOIN (
        SELECT tradepiece, MAX(datetimeid) AS max_datetimeid
        FROM history_tradepieces
        WHERE EXISTS (
            SELECT 1
            FROM active_trades at
            WHERE at.tradepiece = history_tradepieces.tradepiece
        )
        GROUP BY tradepiece
    ) latest
    ON ht.tradepiece = latest.tradepiece AND ht.datetimeid = latest.max_datetimeid
    WHERE CAST(ht.datetimeid AS DATE) = CAST(ht.bookdate AS DATE)
)
SELECT
    CASE WHEN tp.company = 44 THEN 'USG' WHEN tp.company = 45 THEN 'Prime' END AS fund,
    RTRIM(tp.ledgername) AS Series,
    tp.tradepiece AS "Trade ID",
    RTRIM(tt.description) AS TradeType,
    tp.startdate AS "Start Date",
    CASE WHEN tp.closedate IS NULL THEN tp.enddate ELSE tp.closedate END AS "End Date",
    tp.fx_money AS Money,
    LTRIM(RTRIM(tp.contraname)) AS Counterparty,
    COALESCE(tc.lastrate, tp.reporate) AS "Orig. Rate",
    tp.price AS "Orig. Price",
    LTRIM(RTRIM(tp.isin)) AS BondID,
    tp.par * CASE WHEN tp.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(tt.description) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE tp.haircut END AS HairCut,
    tci.commissionvalue * 100 AS Spread,
    LTRIM(RTRIM(tp.acct_number)) AS "cp short",
    CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USG' WHEN tp.tradepiece IN (60320, 60321, 60258) THEN 'BBB' WHEN tp.comments = '' THEN rt.rating ELSE tp.comments END AS Comments,
    tp.fx_money + tc.repointerest_unrealized + tc.repointerest_nbd AS "End Money",
    CASE WHEN RTRIM(is3.description) = 'CLO CRE' THEN 'CMBS' ELSE RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USD Cash' ELSE is2.description END) END AS "Product Type",
    RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'Cash' ELSE is3.description END) AS "Collateral Type"
FROM tradepieces tp
INNER JOIN tradepiececalcdatas tc ON tc.tradepiece = tp.tradepiece
INNER JOIN tradecommissionpieceinfo tci ON tci.tradepiece = tp.tradepiece
INNER JOIN tradetypes tt ON tt.tradetype = tp.shelltradetype
INNER JOIN issues i ON i.cusip = tp.cusip
INNER JOIN currencys c ON c.currency = tp.currency_money
INNER JOIN statusdetails sd ON sd.statusdetail = tp.statusdetail
INNER JOIN statusmains sm ON sm.statusmain = tp.statusmain
INNER JOIN issuecategories ic ON ic.issuecategory = tp.issuecategory
INNER JOIN issuesubtypes1 is1 ON is1.issuesubtype1 = ic.issuesubtype1
INNER JOIN issuesubtypes2 is2 ON is2.issuesubtype2 = ic.issuesubtype2
INNER JOIN issuesubtypes3 is3 ON is3.issuesubtype3 = ic.issuesubtype3
INNER JOIN depositorys d ON tp.depositoryid = d.depositoryid
LEFT JOIN latest_ratings rt ON rt.tradepiece = tp.tradepiece
WHERE tp.statusmain <> 6
AND tp.company IN (44, 45)
AND tt.description IN ('Reverse', 'ReverseFree', 'RepoFree')
AND (tp.STARTDATE <= :valdate) AND (tp.enddate > :valdate OR tp.enddate IS NULL) AND (tp.CLOSEDATE > :valdate OR tp.CLOSEDATE IS NULL)
ORDER BY tp.company ASC, tp.ledgername ASC, tp.contraname ASC;
"""

helix_ratings_query = """
DECLARE @CustomDate DATE;
SET @CustomDate = {date_placeholder}; -- Replace with your actual custom date

WITH active_trades AS (
    SELECT tradepiece
    FROM tradepieces
    WHERE startdate <= @CustomDate
      AND (closedate IS NULL OR closedate >= @CustomDate)
      AND (enddate IS NULL OR enddate >= @CustomDate)
      AND statusmain <> 6
      AND company IN (44, 45)
      AND ledgername = 'Master' -- Ensure only 'Master' ledger is selected
),
latest_ratings AS (
    SELECT ht.tradepiece, ht.comments AS rating, 
           ROW_NUMBER() OVER (PARTITION BY ht.tradepiece ORDER BY ht.datetimeid DESC) AS rn
    FROM history_tradepieces ht
    WHERE EXISTS (
        SELECT 1
        FROM active_trades at
        WHERE at.tradepiece = ht.tradepiece
    )
    AND CAST(ht.datetimeid AS DATE) = CAST(ht.bookdate AS DATE)
)
SELECT DISTINCT
    LTRIM(RTRIM(tp.isin)) AS bond_id,
    COALESCE(
        CASE 
            WHEN tp.comments = '' THEN ISNULL(rt.rating, 'NR') 
            ELSE tp.comments 
        END, 'NR') AS rating
FROM tradepieces tp
LEFT JOIN (
    SELECT tradepiece, rating 
    FROM latest_ratings 
    WHERE rn = 1
) rt ON rt.tradepiece = tp.tradepiece
WHERE tp.STARTDATE <= @CustomDate
  AND (tp.enddate IS NULL OR tp.enddate > @CustomDate)
  AND (tp.CLOSEDATE IS NULL OR tp.CLOSEDATE > @CustomDate)
  AND tp.company IN (44, 45)
  AND tp.statusmain <> 6
  AND tp.ledgername = 'Master' -- Ensure only 'Master' ledger is selected
  AND LTRIM(RTRIM(tp.isin)) NOT LIKE 'CASH%' -- Exclude ISINs that start with 'CASH'
  AND LTRIM(RTRIM(tp.isin)) NOT LIKE 'JP%'; -- Exclude ISINs that start with 'JP'

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

bloomberg_bond_id_query = """
            select distinct
            case when tradepieces.company = 44 then 'USG Fund' when tradepieces.company = 45 then 'Prime Fund' when tradepieces.COMPANY = 46 then 'MMT IM Fund' when tradepieces.COMPANY = 48 then 'LMCP Inv Fund'  when tradepieces.COMPANY = 49 then 'LucidRepo' end Fund,
            ltrim(rtrim(Tradepieces.ISIN)) BondID
            from tradepieces 
            where tradepieces.isvisible = 1
            and tradepieces.company in (44,45,46, 48,49)
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
    case 
        when tradepieces.company = 44 then 'USG' 
        when tradepieces.company = 45 then 'Prime' 
        when tradepieces.company = 46 then 'MMT' 
        when tradepieces.company = 48 then 'LMCP' 
    end "Fund",
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
# Return a list of tradepieces that were entered on @valdate and started on or after @valdate
current_trade_daily_report_helix_trade_query = """
DECLARE @valdate AS DATE
SET @valdate = ?

SELECT
    case 
        when tradepieces.company = 44 then 'USG' 
        when tradepieces.company = 45 then 'Prime' 
        when tradepieces.company = 46 then 'MMT' 
        when tradepieces.company = 48 then 'LMCP' 
    end "Fund",
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
AND tradepieces.statusmain <> 6
ORDER BY tradepieces.company ASC, tradepieces.ledgername ASC, tradepieces.contraname ASC, [Start Date]
"""

# Use for reporting
# This is a list of tradepieces that were entered on @valdate but started before @valdate
as_of_trade_daily_report_helix_trade_query = """
DECLARE @valdate AS DATE
SET @valdate = ?

SELECT
    case 
        when tradepieces.company = 44 then 'USG' 
        when tradepieces.company = 45 then 'Prime' 
        when tradepieces.company = 46 then 'MMT' 
        when tradepieces.company = 48 then 'LMCP' 
    end "Fund",
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
AND tradepieces.statusmain <> 6
ORDER BY tradepieces.company ASC, tradepieces.ledgername ASC, tradepieces.contraname ASC, [Start Date]
"""


HELIX_price_and_factor_by_date = """
DECLARE @CustomDate DATE
SET @CustomDate = {date_placeholder};

WITH LatestRevisionsPrices AS (
    -- Get the latest revision from HISTORY_ISSUEPRICES
    SELECT
        ISSUE,
        MAX(REVISIONID) AS MaxRevisionID
    FROM History_issueprices
    WHERE CAST(DatetimeID AS DATE) = @CustomDate
    GROUP BY ISSUE
),
LatestRevisionsFactorInfo AS (
    -- Get the latest revision from HISTORY_ISSUEFACTORINFO
    SELECT
        ISSUE,
        MAX(REVISIONID) AS MaxRevisionID
    FROM HISTORY_ISSUEFACTORINFO
    WHERE CAST(DatetimeID AS DATE) = @CustomDate
    GROUP BY ISSUE
)
SELECT
    h.ISSUE,
    LTRIM(RTRIM(i.isin)) AS BondID,
    h.CURRENTREPOPRICE AS Helix_price,
    f.FACTOR as Helix_factor,
    CAST(h.DATETIMEID as DATE) AS Data_date
FROM History_issueprices h
JOIN LatestRevisionsPrices lr
    ON h.ISSUE = lr.ISSUE
    AND h.REVISIONID = lr.MaxRevisionID
JOIN ISSUES i
    ON h.ISSUE = i.ISSUE
JOIN HISTORY_ISSUEFACTORINFO f
    ON h.ISSUE = f.ISSUE
    AND CAST(h.DatetimeID AS DATE) = CAST(f.DatetimeID AS DATE)  -- Match ISSUE and DatetimeID
JOIN LatestRevisionsFactorInfo lf
    ON f.ISSUE = lf.ISSUE
    AND f.REVISIONID = lf.MaxRevisionID
WHERE CAST(h.DatetimeID AS DATE) = @CustomDate
"""

HELIX_historical_price = """
WITH LatestRevisionsPrices AS (
    -- Get the latest revision from HISTORY_ISSUEPRICES for all dates
    SELECT
        ISSUE,
        CAST(DatetimeID AS DATE) AS Data_date,  -- Include the date in the grouping
        MAX(REVISIONID) AS MaxRevisionID
    FROM History_issueprices
    GROUP BY ISSUE, CAST(DatetimeID AS DATE)  -- Group by both ISSUE and date
)
SELECT
    h.ISSUE,
    LTRIM(RTRIM(i.isin)) AS BondID,
    h.CURRENTREPOPRICE AS Helix_price,
    CAST(h.DATETIMEID AS DATE) AS Data_date
FROM History_issueprices h
JOIN LatestRevisionsPrices lr
    ON h.ISSUE = lr.ISSUE
    AND CAST(h.DatetimeID AS DATE) = lr.Data_date
    AND h.REVISIONID = lr.MaxRevisionID
JOIN ISSUES i
    ON h.ISSUE = i.ISSUE
"""


HELIX_current_factor = """
SELECT DISTINCT 
    LTRIM(RTRIM(tradepieces.ISIN)) AS BondID,
    tradepiececalcdatas.CURRENTMBSFACTOR AS Helix_factor
FROM 
    tradepieces
INNER JOIN 
    TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
WHERE 
    tradepieces.company IN (44, 45, 46)
    AND NOT (tradepieces.company = 45 AND LTRIM(RTRIM(tradepieces.ledgername)) <> 'Master')
    AND tradepieces.statusmain <> 6
    AND (tradepieces.startdate <= GETDATE() AND 
         (COALESCE(tradepieces.closedate, tradepieces.enddate) > GETDATE() OR 
          COALESCE(tradepieces.closedate, tradepieces.enddate) IS NULL))
ORDER BY 
    BondID;
"""
# TODO: SHOULD USE THIS FOR ALL QUERY FORMAT
AUM_query = """
DECLARE @CustomDate DATE;
SET @CustomDate = {date_placeholder};
DECLARE @EurFxRate FLOAT = 1.0894;
DECLARE @SumNAVLastRoll_USD FLOAT;

WITH TradeData AS (
    SELECT
        Tradepieces.TRADEPIECE AS "Trade ID",
        Tradepieces.LEDGERNAME AS "Ledger",
        TRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
        Tradepieces.TRADEDATE AS "Trade Date",
        Tradepieces.STARTDATE AS "Start Date",
        CASE
            WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
            ELSE Tradepieces.CLOSEDATE
        END AS "End Date",
        - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION AS "Money",
        CURRENCYS.CURRENCYCODE AS "Currency",
        TRIM(Tradepieces.CONTRANAME) AS "Counterparty",
        Tradepieces.REPORATE AS "Orig. Rate",
        TRADEPIECECALCDATAS.todayrate AS "Today Rate",
        TRADEPIECECALCDATAS.REPOINTEREST_ONEDAY AS "Daily Int.",
        TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Accrued Int.",
        TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED AS "Unrealized Int.",
        TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Total Interest Due",
        (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "Crnt Money Due",
        (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED) + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "End Money",
        TRIM(Tradepieces.ISIN) AS "BondID",
        - Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Par/Quantity",
        Tradepiececalcdatas.CURRENTMBSFACTOR AS "Issue Factor",
        - Tradepiececalcdatas.CURRENTMBSFACTOR * Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Current Face",
        TRADEPIECECALCDATAS.CURRENTPRICE AS "Current Price",
        - TRADEPIECECALCDATAS.CURRENTMARKETVALUE * TRADETYPES.PAR_DIRECTION AS "Mkt Value",
        Tradepieces.PRICE AS "Repo Price",
        Tradepieces.PRICEFULL AS "Trade Price",
        Tradepieces.HAIRCUT AS "HairCut",
        CASE
            WHEN Tradepieces.HAIRCUTMETHOD = 0 THEN 'No Haircut'
            WHEN Tradepieces.HAIRCUTMETHOD = 1 THEN '% Proceeds'
            WHEN Tradepieces.HAIRCUTMETHOD = 2 THEN 'Adj By %'
            WHEN Tradepieces.HAIRCUTMETHOD = 3 THEN 'Adj By Points'
            ELSE Tradepieces.X_SPECIALCODE1
        END AS "Haircut Method",
        RTRIM(CASE
            WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
            ELSE ISSUESUBTYPES2.DESCRIPTION
        END) AS "Product Type",
        RTRIM(CASE
            WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD'
            ELSE ISSUESUBTYPES1.DESCRIPTION
        END) AS "Product",
        Tradepieces.USERNAME AS "User",
        RTRIM(ISNULL(Tradepieces.DEPOSITORY, '')) AS "Depository",
        RTRIM(STATUSDETAILS.DESCRIPTION) AS "Status Detail",
        TRIM(Tradepieces.ACCT_NUMBER) AS "CP Short Name",
        RTRIM(STATUSMAINS.DESCRIPTION) AS "Status Main",
        RTRIM(CASE
            WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
            WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'Cash'
            ELSE ISSUESUBTYPES3.DESCRIPTION
        END) AS "Product Sub",
        RTRIM(CASE
            WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
            WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'EUR Cash'
            ELSE ISNULL(ISSUES.DESCRIPTION_1, '')
        END) AS "Description",
        TRADEPIECEXREFS.FRONTOFFICEID AS "Facility",
        Tradepieces.COMMENTS AS "Comments",
        Tradecommissionpieceinfo.commissionvalue AS "Commission",
        ISNULL(Tradepieces.STRATEGY, '') AS "Fund Entity",
        CASE
            WHEN Tradepieces.STARTDATE > @CustomDate OR Tradepieces.ENDDATE <= @CustomDate THEN 0
            ELSE 1
        END AS "IsVisible",
        CASE
            WHEN TRIM(TRADETYPES.DESCRIPTION) = 'Reverse' THEN - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION *
                CASE
                    WHEN Tradepieces.STARTDATE > @CustomDate OR Tradepieces.ENDDATE <= @CustomDate THEN 0
                    ELSE 1
                END
            ELSE 0
        END AS "NAVLastRoll_LC",
        CASE
            WHEN TRIM(TRADETYPES.DESCRIPTION) = 'Reverse' THEN - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION *
                CASE
                    WHEN Tradepieces.STARTDATE > @CustomDate OR Tradepieces.ENDDATE <= @CustomDate THEN 0
                    ELSE 1
                END *
                CASE
                    WHEN CURRENCYS.CURRENCYCODE = 'EUR' THEN @EurFxRate
                    ELSE 1
                END
            ELSE 0
        END AS "NAVLastRoll_USD"
    FROM
        tradepieces
        INNER JOIN TRADEPIECEXREFS ON TRADEPIECEXREFS.TRADEPIECE = TRADEPIECES.TRADEPIECE
        INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
        INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
        INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
        INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
        INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_PAR
        INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
        INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
        INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
        INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
        INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
        INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
    WHERE
        tradepieces.company = 48
        AND tradepieces.statusmain <> 6
        AND tradepieces.STARTDATE > DATEADD(DAY, -180, @CustomDate)
        AND TRIM(TRADETYPES.DESCRIPTION) = 'Reverse'
),
CTE AS (
    SELECT
        Tradepieces.TRADEPIECE AS "Trade ID",
        Tradepieces.LEDGERNAME AS "Ledger",
        TRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
        Tradepieces.TRADEDATE AS "Trade Date",
        Tradepieces.STARTDATE AS "Start Date",
        CASE
            WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
            ELSE Tradepieces.CLOSEDATE
        END AS "End Date",
        - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION AS "Money",
        CURRENCYS.CURRENCYCODE AS "Currency",
        TRIM(Tradepieces.CONTRANAME) AS "Counterparty",
        Tradepieces.REPORATE AS "Orig. Rate",
        TRADEPIECECALCDATAS.todayrate AS "Today Rate",
        TRADEPIECECALCDATAS.REPOINTEREST_ONEDAY AS "Daily Int.",
        TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Accrued Int.",
        TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED AS "Unrealized Int.",
        TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Total Interest Due",
        (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "Crnt Money Due",
        (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED) + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "End Money",
        TRIM(Tradepieces.ISIN) AS "BondID",
        - Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Par/Quantity",
        Tradepiececalcdatas.CURRENTMBSFACTOR AS "Issue Factor",
        - Tradepiececalcdatas.CURRENTMBSFACTOR * Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Current Face",
        TRADEPIECECALCDATAS.CURRENTPRICE AS "Current Price",
        - TRADEPIECECALCDATAS.CURRENTMARKETVALUE * TRADETYPES.PAR_DIRECTION AS "Mkt Value",
        Tradepieces.PRICE AS "Repo Price",
        Tradepieces.PRICEFULL AS "Trade Price",
        Tradepieces.HAIRCUT AS "HairCut",
        CASE
            WHEN Tradepieces.HAIRCUTMETHOD = 0 THEN 'No Haircut'
            WHEN Tradepieces.HAIRCUTMETHOD = 1 THEN '% Proceeds'
            WHEN Tradepieces.HAIRCUTMETHOD = 2 THEN 'Adj By %'
            WHEN Tradepieces.HAIRCUTMETHOD = 3 THEN 'Adj By Points'
            ELSE Tradepieces.X_SPECIALCODE1
        END AS "Haircut Method",
        RTRIM(CASE
            WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
            WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'EUR Cash'
            ELSE ISNULL(ISSUES.DESCRIPTION_1, '')
        END) AS "Description",
        TRADEPIECEXREFS.FRONTOFFICEID AS "Facility",
        Tradepieces.COMMENTS AS "Comments",
        Tradecommissionpieceinfo.commissionvalue AS "Commission",
        ISNULL(Tradepieces.STRATEGY, '') AS "Fund Entity",
        CASE
            WHEN Tradepieces.STARTDATE > @CustomDate OR (CASE
                WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
                ELSE Tradepieces.CLOSEDATE
            END) <= @CustomDate THEN 0
            ELSE 1
        END AS "IsVisibleAsOf",
        CASE
            WHEN Tradepieces.STARTDATE > @CustomDate OR (CASE
                WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
                ELSE Tradepieces.CLOSEDATE
            END) <= @CustomDate THEN 0
            ELSE - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION
        END AS "NAVLastRoll",
        CASE
            WHEN Tradepieces.STARTDATE > @CustomDate OR (CASE
                WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
                ELSE Tradepieces.CLOSEDATE
            END) <= @CustomDate THEN 0
            ELSE (DATEDIFF(DAY, Tradepieces.STARTDATE, @CustomDate) * 1.0 /
            CASE
                WHEN LEFT(Tradepieces.LEDGERNAME, 3) = 'USG' THEN 365
                ELSE 360
            END) * Tradepieces.REPORATE / 100 * (- Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION)
        END AS "AccruedAsOf",
        CASE
            WHEN Tradepieces.STARTDATE > @CustomDate OR (CASE
                WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
                ELSE Tradepieces.CLOSEDATE
            END) <= @CustomDate THEN 0
            ELSE (DATEDIFF(DAY, Tradepieces.STARTDATE, @CustomDate) * 1.0 /
            CASE
                WHEN LEFT(Tradepieces.LEDGERNAME, 3) = 'USG' THEN 365
                ELSE 360
            END) * Tradepieces.REPORATE / 100 * (- Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION) +
            (- Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION)
        END AS "NAVAsOf"
    FROM
        tradepieces
        INNER JOIN TRADEPIECEXREFS ON TRADEPIECEXREFS.TRADEPIECE = TRADEPIECES.TRADEPIECE
        INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
        INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
        INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
        INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
        INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_PAR
        INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
        INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
        INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
        INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
        INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
        INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
    WHERE
        tradepieces.company = 49
        AND tradepieces.statusmain <> 6
        AND tradepieces.STARTDATE > DATEADD(DAY, -180, @CustomDate)
),
SummaryData AS (
    SELECT
        TRIM(CTE."BondID") AS "BondID",
        CASE
            WHEN TRIM(CTE."BondID") = 'PRIME-M000' THEN 'Series M'
            WHEN TRIM(CTE."BondID") = 'PRIME-MIG0' THEN 'Series MIG'
            WHEN TRIM(CTE."BondID") = 'PRIME-Q100' THEN 'Series Q1'
            WHEN TRIM(CTE."BondID") = 'PRIME-QX00' THEN 'Series QX'
            WHEN TRIM(CTE."BondID") = 'PRIME-Q364' THEN 'Series Q364'
            WHEN TRIM(CTE."BondID") = 'PRIME-2YIG' THEN 'Series 2YIG'
            WHEN TRIM(CTE."BondID") = 'PRIME-A100' THEN 'Series A1'
            WHEN TRIM(CTE."BondID") = 'PRIME-A2Y0' THEN 'Series A2Y'
            WHEN TRIM(CTE."BondID") = 'PRIME-C100' THEN 'Series C1'
            WHEN TRIM(CTE."BondID") = 'USGFD-M000' THEN 'USG M'
            WHEN TRIM(CTE."BondID") = 'PRIME-USGM' THEN 'Series USGM'
        END AS "Series",
        LEFT(TRIM(CTE."BondID"), 9) AS "Series ID",
        ABS(SUM(CTE."NAVAsOf")) AS "Outstanding"
    FROM
        CTE
    WHERE
        TRIM(CTE."BondID") IN ('PRIME-M000', 'PRIME-MIG0', 'PRIME-Q100', 'PRIME-QX00', 'PRIME-Q364', 'PRIME-2YIG', 'PRIME-A100', 'PRIME-C100', 'USGFD-M000','PRIME-A2Y0','PRIME-USGM')
    GROUP BY
        TRIM(CTE."BondID")
),
OtherMandatesData AS (
SELECT SUM(NAVLastRoll_USD) AS SumNAVLastRoll_USD
FROM TradeData
)
SELECT
"BondID",
"Series",
"Series ID",
"Outstanding"
FROM
SummaryData
UNION ALL
SELECT
'MMT-000000' AS "BondID",
'Other Mandates' AS "Series",
LEFT('MMT-000000', 9) AS "Series ID",
COALESCE(OtherMandatesData.SumNAVLastRoll_USD, 0) AS "Outstanding"
FROM
OtherMandatesData;
"""

# TODO: This should be used with execute_query_v2
active_trade_by_date_helix_query = """
DECLARE @CustomDate DATE;
SET @CustomDate = {date_placeholder}; -- Replace with your actual date

WITH active_trades AS (
    SELECT tradepiece
    FROM tradepieces
    WHERE startdate <= @CustomDate
    AND (closedate IS NULL OR closedate >= @CustomDate OR enddate >= @CustomDate)
),
latest_ratings AS (
    SELECT ht.tradepiece, ht.comments AS rating
    FROM history_tradepieces ht
    JOIN (
        SELECT tradepiece, MAX(datetimeid) AS max_datetimeid
        FROM history_tradepieces
        WHERE EXISTS (
            SELECT 1
            FROM active_trades at
            WHERE at.tradepiece = history_tradepieces.tradepiece
        )
        GROUP BY tradepiece
    ) latest
    ON ht.tradepiece = latest.tradepiece AND ht.datetimeid = latest.max_datetimeid
    WHERE CAST(ht.datetimeid AS DATE) = CAST(ht.bookdate AS DATE)
)
SELECT
    CASE WHEN tp.company = 44 THEN 'USG' WHEN tp.company = 45 THEN 'Prime' END AS fund,
    RTRIM(tp.ledgername) AS Series,
    tp.tradepiece AS "Trade ID",
    RTRIM(tt.description) AS TradeType,
    tp.startdate AS "Start Date",
    CASE WHEN tp.closedate IS NULL THEN tp.enddate ELSE tp.closedate END AS "End Date",
    tp.fx_money AS Money,
    LTRIM(RTRIM(tp.contraname)) AS Counterparty,
    COALESCE(tc.lastrate, tp.reporate) AS "Orig. Rate",
    tp.price AS "Orig. Price",
    LTRIM(RTRIM(tp.isin)) AS BondID,
    tp.par * CASE WHEN tp.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(tt.description) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE tp.haircut END AS HairCut,
    tci.commissionvalue * 100 AS Spread,
    LTRIM(RTRIM(tp.acct_number)) AS "cp short",
    CASE 
        WHEN tp.cusip = 'CASHUSD01' THEN 'USG' 
        WHEN tp.tradepiece IN (60320, 60321, 60258) THEN 'BBB' 
        WHEN tp.comments = '' THEN rt.rating 
        ELSE tp.comments 
    END AS Comments,
    tp.fx_money + tc.repointerest_unrealized + tc.repointerest_nbd AS "End Money",
    CASE 
        WHEN RTRIM(is3.description) = 'CLO CRE' THEN 'CMBS' 
        ELSE RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USD Cash' ELSE is2.description END) 
    END AS "Product Type",
    RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'Cash' ELSE is3.description END) AS "Collateral Type"
FROM tradepieces tp
INNER JOIN tradepiececalcdatas tc ON tc.tradepiece = tp.tradepiece
INNER JOIN tradecommissionpieceinfo tci ON tci.tradepiece = tp.tradepiece
INNER JOIN tradetypes tt ON tt.tradetype = tp.shelltradetype
INNER JOIN issues i ON i.cusip = tp.cusip
INNER JOIN currencys c ON c.currency = tp.currency_money
INNER JOIN statusdetails sd ON sd.statusdetail = tp.statusdetail
INNER JOIN statusmains sm ON sm.statusmain = tp.statusmain
INNER JOIN issuecategories ic ON ic.issuecategory = tp.issuecategory
INNER JOIN issuesubtypes1 is1 ON is1.issuesubtype1 = ic.issuesubtype1
INNER JOIN issuesubtypes2 is2 ON is2.issuesubtype2 = ic.issuesubtype2
INNER JOIN issuesubtypes3 is3 ON is3.issuesubtype3 = ic.issuesubtype3
INNER JOIN depositorys d ON tp.depositoryid = d.depositoryid
LEFT JOIN latest_ratings rt ON rt.tradepiece = tp.tradepiece
WHERE tp.statusmain <> 6
AND tp.company IN (44, 45)
AND tt.description IN ('Reverse', 'ReverseFree', 'RepoFree')
AND tp.STARTDATE <= @CustomDate
AND (tp.enddate > @CustomDate OR tp.enddate IS NULL)
AND (tp.CLOSEDATE > @CustomDate OR tp.CLOSEDATE IS NULL)
ORDER BY tp.company ASC, tp.ledgername ASC, tp.contraname ASC;
"""

counterparty_count_summary = """
WITH active_trades AS (
    SELECT tradepiece
    FROM tradepieces
    WHERE startdate <= :valdate
    AND (closedate IS NULL OR closedate >= :valdate OR enddate >= :valdate)
),
latest_ratings AS (
    SELECT ht.tradepiece, ht.comments AS rating
    FROM history_tradepieces ht
    JOIN (
        SELECT tradepiece, MAX(datetimeid) AS max_datetimeid
        FROM history_tradepieces
        WHERE EXISTS (
            SELECT 1
            FROM active_trades at
            WHERE at.tradepiece = history_tradepieces.tradepiece
        )
        GROUP BY tradepiece
    ) latest
    ON ht.tradepiece = latest.tradepiece AND ht.datetimeid = latest.max_datetimeid
    WHERE CAST(ht.datetimeid AS DATE) = CAST(ht.bookdate AS DATE)
)
SELECT
    CASE WHEN tp.company = 44 THEN 'USG' WHEN tp.company = 45 THEN 'Prime' END AS fund,
    RTRIM(tp.ledgername) AS Series,
    COUNT(DISTINCT LTRIM(RTRIM(tp.contraname))) AS "Unique Counterparties",
    SUM(tp.fx_money) AS "Total Money"
FROM tradepieces tp
INNER JOIN tradepiececalcdatas tc ON tc.tradepiece = tp.tradepiece
INNER JOIN tradecommissionpieceinfo tci ON tci.tradepiece = tp.tradepiece
INNER JOIN tradetypes tt ON tt.tradetype = tp.shelltradetype
INNER JOIN issues i ON i.cusip = tp.cusip
INNER JOIN currencys c ON c.currency = tp.currency_money
INNER JOIN statusdetails sd ON sd.statusdetail = tp.statusdetail
INNER JOIN statusmains sm ON sm.statusmain = tp.statusmain
INNER JOIN issuecategories ic ON ic.issuecategory = tp.issuecategory
INNER JOIN issuesubtypes1 is1 ON is1.issuesubtype1 = ic.issuesubtype1
INNER JOIN issuesubtypes2 is2 ON is2.issuesubtype2 = ic.issuesubtype2
INNER JOIN issuesubtypes3 is3 ON is3.issuesubtype3 = ic.issuesubtype3
INNER JOIN depositorys d ON tp.depositoryid = d.depositoryid
LEFT JOIN latest_ratings rt ON rt.tradepiece = tp.tradepiece
WHERE tp.statusmain <> 6
AND tp.company IN (44, 45)
AND tt.description IN ('Reverse', 'ReverseFree', 'RepoFree')
AND (tp.STARTDATE <= :valdate) AND (tp.enddate > :valdate OR tp.enddate IS NULL) AND (tp.CLOSEDATE > :valdate OR tp.CLOSEDATE IS NULL)
GROUP BY
    CASE WHEN tp.company = 44 THEN 'USG' WHEN tp.company = 45 THEN 'Prime' END,
    RTRIM(tp.ledgername)
ORDER BY fund ASC, Series ASC;
"""

current_trade_subscriptions_redemptions_querry = """
SELECT
    Tradepieces.TRADEPIECE AS "Trade ID",
    Tradepieces.LEDGERNAME AS "Ledger",
    TRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
    Tradepieces.TRADEDATE AS "Trade Date",
    Tradepieces.STARTDATE AS "Start Date",
    CASE
        WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
        ELSE Tradepieces.CLOSEDATE
    END AS "End Date",
    - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION AS "Money",
    CURRENCYS.CURRENCYCODE AS "Currency",
    TRIM(Tradepieces.CONTRANAME) AS "Counterparty",
    Tradepieces.REPORATE AS "Orig. Rate",
    TRADEPIECECALCDATAS.todayrate AS "Today Rate",
    TRADEPIECECALCDATAS.REPOINTEREST_ONEDAY AS "Daily Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Accrued Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED AS "Unrealized Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Total Interest Due",
    (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "Crnt Money Due",
    (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED) + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "End Money",
    TRIM(Tradepieces.ISIN) AS "BondID",
    - Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Par/Quantity",
    Tradepiececalcdatas.CURRENTMBSFACTOR AS "Issue Factor",
    - Tradepiececalcdatas.CURRENTMBSFACTOR * Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Current Face",
    TRADEPIECECALCDATAS.CURRENTPRICE AS "Current Price",
    - TRADEPIECECALCDATAS.CURRENTMARKETVALUE * TRADETYPES.PAR_DIRECTION AS "Mkt Value",
    Tradepieces.PRICE AS "Repo Price",
    Tradepieces.PRICEFULL AS "Trade Price",
    Tradepieces.HAIRCUT AS "HairCut",
    CASE
        WHEN Tradepieces.HAIRCUTMETHOD = 0 THEN 'No Haircut'
        WHEN Tradepieces.HAIRCUTMETHOD = 1 THEN '% Proceeds'
        WHEN Tradepieces.HAIRCUTMETHOD = 2 THEN 'Adj By %'
        WHEN Tradepieces.HAIRCUTMETHOD = 3 THEN 'Adj By Points'
        ELSE Tradepieces.X_SPECIALCODE1
    END AS "Haircut Method",
    RTRIM(CASE
        WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
        ELSE ISSUESUBTYPES2.DESCRIPTION
    END) AS "Product Type",
    RTRIM(CASE
        WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD'
        ELSE ISSUESUBTYPES1.DESCRIPTION
    END) AS "Product",
    Tradepieces.USERNAME AS "User",
       RTRIM(ISNULL(Tradepieces.DEPOSITORY, '')) AS "Depository",
       RTRIM(STATUSDETAILS.DESCRIPTION) AS "Status Detail",
       TRIM(Tradepieces.ACCT_NUMBER) AS "CP Short Name",
       RTRIM(STATUSMAINS.DESCRIPTION) AS "Status Main",
       RTRIM(CASE
             WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
             WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'Cash'
             ELSE ISSUESUBTYPES3.DESCRIPTION
       END) AS "Product Sub",
       RTRIM(CASE
             WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
             WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'EUR Cash'
             ELSE ISNULL(ISSUES.DESCRIPTION_1, '')
       END) AS "Description",
       TRADEPIECEXREFS.FRONTOFFICEID AS "Facility",
       Tradepieces.COMMENTS AS "Comments",
       Tradecommissionpieceinfo.commissionvalue AS "Commission",
       ISNULL(Tradepieces.STRATEGY, '') AS "Fund Entity",
       CAST(tradepieces.ENTERDATETIMEID AS DATE) AS "Enter Date"
       FROM
       tradepieces
       INNER JOIN TRADEPIECEXREFS ON TRADEPIECEXREFS.TRADEPIECE = TRADEPIECES.TRADEPIECE
       INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
       INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
       INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
       INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
       INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_PAR
       INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
       INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
       INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
       INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
       INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
       INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
       WHERE
       tradepieces.company = 49
       AND tradepieces.statusmain <> 6 and tradepieces.STARTDATE > DATEADD(DAY, -180, GETDATE())
"""

# Might be deprecated based on how we can interactively use parameters
active_trade_as_of_custom_date = """
DECLARE @valdate DATE = '2024-07-24';

WITH active_trades AS (
    SELECT tradepiece
    FROM tradepieces
    WHERE startdate <= @valdate
    AND (closedate IS NULL OR closedate >= @valdate OR enddate >= @valdate)
),
latest_ratings AS (
    SELECT ht.tradepiece, ht.comments AS rating
    FROM history_tradepieces ht
    JOIN (
        SELECT tradepiece, MAX(datetimeid) AS max_datetimeid
        FROM history_tradepieces
        WHERE EXISTS (
            SELECT 1
            FROM active_trades at
            WHERE at.tradepiece = history_tradepieces.tradepiece
        )
        GROUP BY tradepiece
    ) latest
    ON ht.tradepiece = latest.tradepiece AND ht.datetimeid = latest.max_datetimeid
    WHERE CAST(ht.datetimeid AS DATE) = CAST(ht.bookdate AS DATE)
)
SELECT TOP 100
    CASE WHEN tp.company = 44 THEN 'USG' WHEN tp.company = 45 THEN 'Prime' END AS fund,
    RTRIM(tp.ledgername) AS Series,
    tp.tradepiece AS "Trade ID",
    RTRIM(tt.description) AS TradeType,
    tp.startdate AS "Start Date",
    CASE WHEN tp.closedate IS NULL THEN tp.enddate ELSE tp.closedate END AS "End Date",
    tp.fx_money AS Money,
    LTRIM(RTRIM(tp.contraname)) AS Counterparty,
    COALESCE(tc.lastrate, tp.reporate) AS "Orig. Rate",
    tp.price AS "Orig. Price",
    LTRIM(RTRIM(tp.isin)) AS BondID,
    tp.par * CASE WHEN tp.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(tt.description) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE tp.haircut END AS HairCut,
    tci.commissionvalue * 100 AS Spread,
    LTRIM(RTRIM(tp.acct_number)) AS "cp short",
    CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USG' WHEN tp.tradepiece IN (60320, 60321, 60258) THEN 'BBB' WHEN tp.comments = '' THEN rt.rating ELSE tp.comments END AS Comments,
    tp.fx_money + tc.repointerest_unrealized + tc.repointerest_nbd AS "End Money",
    CASE WHEN RTRIM(is3.description) = 'CLO CRE' THEN 'CMBS' ELSE RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USD Cash' ELSE is2.description END) END AS "Product Type",
    RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'Cash' ELSE is3.description END) AS "Collateral Type"
FROM tradepieces tp
INNER JOIN tradepiececalcdatas tc ON tc.tradepiece = tp.tradepiece
INNER JOIN tradecommissionpieceinfo tci ON tci.tradepiece = tp.tradepiece
INNER JOIN tradetypes tt ON tt.tradetype = tp.shelltradetype
INNER JOIN issues i ON i.cusip = tp.cusip
INNER JOIN currencys c ON c.currency = tp.currency_money
INNER JOIN statusdetails sd ON sd.statusdetail = tp.statusdetail
INNER JOIN statusmains sm ON sm.statusmain = tp.statusmain
INNER JOIN issuecategories ic ON ic.issuecategory = tp.issuecategory
INNER JOIN issuesubtypes1 is1 ON is1.issuesubtype1 = ic.issuesubtype1
INNER JOIN issuesubtypes2 is2 ON is2.issuesubtype2 = ic.issuesubtype2
INNER JOIN issuesubtypes3 is3 ON is3.issuesubtype3 = ic.issuesubtype3
INNER JOIN depositorys d ON tp.depositoryid = d.depositoryid
LEFT JOIN latest_ratings rt ON rt.tradepiece = tp.tradepiece
WHERE tp.statusmain <> 6
AND tp.company IN (44, 45)
AND tt.description IN ('Reverse', 'ReverseFree', 'RepoFree')
AND (tp.STARTDATE <= @valdate) AND (tp.enddate > @valdate OR tp.enddate IS NULL) AND (tp.CLOSEDATE > @valdate OR tp.CLOSEDATE IS NULL)
ORDER BY tp.company ASC, tp.ledgername ASC, tp.contraname ASC;
"""


# This is coming from HELIX - Mattias want to join this and the investors table
# Part 1
mattias_query_counterparty = """
SELECT
    Tradepieces.TRADEPIECE AS "Trade ID",
    Tradepieces.LEDGERNAME AS "Ledger",
    TRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
    Tradepieces.TRADEDATE AS "Trade Date",
    Tradepieces.STARTDATE AS "Start Date",
    CASE
        WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
        ELSE Tradepieces.CLOSEDATE
    END AS "End Date",
    - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION AS "Money",
    CURRENCYS.CURRENCYCODE AS "Currency",
    TRIM(Tradepieces.CONTRANAME) AS "Counterparty", -- > Want this FIELD TO BE THE LEGAL ENTITY NAME FROM INVESTORS TABLE (based on mapping of ContraName from TradePieces to Helix Code from Investors)
    Tradepieces.REPORATE AS "Orig. Rate",
    TRADEPIECECALCDATAS.todayrate AS "Today Rate",
    TRADEPIECECALCDATAS.REPOINTEREST_ONEDAY AS "Daily Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Accrued Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED AS "Unrealized Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Total Interest Due",
    (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "Crnt Money Due",
    (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED) + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "End Money",
    TRIM(Tradepieces.ISIN) AS "BondID",
    - Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Par/Quantity",
    Tradepiececalcdatas.CURRENTMBSFACTOR AS "Issue Factor",
    - Tradepiececalcdatas.CURRENTMBSFACTOR * Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Current Face",
    TRADEPIECECALCDATAS.CURRENTPRICE AS "Current Price",
    - TRADEPIECECALCDATAS.CURRENTMARKETVALUE * TRADETYPES.PAR_DIRECTION AS "Mkt Value",
    Tradepieces.PRICE AS "Repo Price",
    Tradepieces.PRICEFULL AS "Trade Price",
    Tradepieces.HAIRCUT AS "HairCut",
    CASE
        WHEN Tradepieces.HAIRCUTMETHOD = 0 THEN 'No Haircut'
        WHEN Tradepieces.HAIRCUTMETHOD = 1 THEN '% Proceeds'
        WHEN Tradepieces.HAIRCUTMETHOD = 2 THEN 'Adj By %'
        WHEN Tradepieces.HAIRCUTMETHOD = 3 THEN 'Adj By Points'
        ELSE Tradepieces.X_SPECIALCODE1
    END AS "Haircut Method",
    RTRIM(CASE
        WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
        ELSE ISSUESUBTYPES2.DESCRIPTION
    END) AS "Product Type",
    RTRIM(CASE
        WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD'
        ELSE ISSUESUBTYPES1.DESCRIPTION
    END) AS "Product",
    Tradepieces.USERNAME AS "User",
       RTRIM(ISNULL(Tradepieces.DEPOSITORY, '')) AS "Depository",
       RTRIM(STATUSDETAILS.DESCRIPTION) AS "Status Detail",
       TRIM(Tradepieces.ACCT_NUMBER) AS "CP Short Name",
       RTRIM(STATUSMAINS.DESCRIPTION) AS "Status Main",
       RTRIM(CASE
             WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
             WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'Cash'
             ELSE ISSUESUBTYPES3.DESCRIPTION
       END) AS "Product Sub",
       RTRIM(CASE
             WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
             WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'EUR Cash'
             ELSE ISNULL(ISSUES.DESCRIPTION_1, '')
       END) AS "Description",
       TRADEPIECEXREFS.FRONTOFFICEID AS "Facility",
       Tradepieces.COMMENTS AS "Comments",
       Tradecommissionpieceinfo.commissionvalue AS "Commission",
       ISNULL(Tradepieces.STRATEGY, '') AS "Fund Entity"
       FROM
       tradepieces
       INNER JOIN TRADEPIECEXREFS ON TRADEPIECEXREFS.TRADEPIECE = TRADEPIECES.TRADEPIECE
       INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
       INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
       INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
       INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
       INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_PAR
       INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
       INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
       INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
       INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
       INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
       INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
       WHERE
       tradepieces.company = 49
       AND tradepieces.statusmain <> 6 and tradepieces.STARTDATE > DATEADD(DAY, -180, GETDATE())
"""

# Part 2
investors_db_query = """
SELECT 
    investors."Legal entity", 
    investors."Helix Code"
FROM 
    investors
"""

## PRICE PX REPORT ##
price_report_helix_query = """
select tradepieces.tradepiece, tradepieces.TRADESHELL, tradepieces.TRADETYPE,
tradepieces.company,ltrim(rtrim(tradepieces.contraname)) contraname, tradepieces.startdate,
coalesce(tradepieces.closedate, tradepieces.enddate) "End Date",
Tradepieces.FX_MONEY as "Money",
TRADEPIECECALCDATAS.TODAYRATE, trim(tradepieces.ISIN) isin,
tradepieces.HAIRCUT, tradepieces.PAR, 
tradepiececalcdatas.CURRENTMBSFACTOR "CURRFACTOR",
tradepieces.MONEY, tradepiececalcdatas.CURRENTPRICE, ltrim(rtrim(tradepieces.acct_number)) acct_number,
case when tradepieces.currency_par = 60 then 'USD' when tradepieces.currency_par = 69 then 'EUR' else null end currency,
TRADEPIECECALCDATAS.REPOINTEREST_NBD as "Accrued Int", 
Tradepieces.PAR * tradepiececalcdatas.CURRENTPRICE * tradepiececalcdatas.CURRENTMBSFACTOR/100 as "Market Value",
ISSUESUBTYPES3.DESCRIPTION "Collateral Type",
tradepieces.comments as "Rating",
ISSUES.description_1 as "Product Type"
from tradepieces 
INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE=TRADEPIECES.TRADEPIECE
INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE=TRADEPIECES.TRADEPIECE
INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE=TRADEPIECES.SHELLTRADETYPE
INNER JOIN ISSUES ON ISSUES.CUSIP=TRADEPIECEs.CUSIP
INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY=TRADEPIECES.CURRENCY_MONEY
INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL=TRADEPIECES.STATUSDETAIL
INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN=TRADEPIECES.STATUSMAIN
INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY=TRADEPIECES.ISSUECATEGORY
INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3=ISSUECATEGORIES.ISSUESUBTYPE3
where
tradepieces.company in (44,45,46)
and not (tradepieces.company = 45 and ltrim(rtrim(tradepieces.ledgername)) <> 'Master')
and tradepieces.statusmain <> 6
and (tradepieces.startdate <= getDate() and ( coalesce(tradepieces.closedate, tradepieces.enddate) > getdate() or coalesce(tradepieces.closedate, tradepieces.enddate) is null) )
order by tradepieces.company, tradepieces.contraname, tradepieces.startdate
"""


# TRANSACTION REC REPORT
transaction_rec_report_helix_trade_query = """
DECLARE @CustomDate DATE;
SET @CustomDate = {date_placeholder};

SELECT
    Tradepieces.TRADEPIECE AS "Trade ID",
    Tradepieces.LEDGERNAME AS "Ledger",
    TRIM(TRADETYPES.DESCRIPTION) AS "TradeType",
    Tradepieces.TRADEDATE AS "Trade Date",
    Tradepieces.STARTDATE AS "Start Date",
    CASE
        WHEN Tradepieces.CLOSEDATE is NULL THEN Tradepieces.ENDDATE
        ELSE Tradepieces.CLOSEDATE
    END AS "End Date",
    - Tradepieces.MONEY * TRADETYPES.MONEY_DIRECTION AS "Money",
    CURRENCYS.CURRENCYCODE AS "Currency",
    TRIM(Tradepieces.CONTRANAME) AS "Counterparty",
    Tradepieces.REPORATE AS "Orig. Rate",
    TRADEPIECECALCDATAS.todayrate AS "Today Rate",
    TRADEPIECECALCDATAS.REPOINTEREST_ONEDAY AS "Daily Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Accrued Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED AS "Unrealized Int.",
    TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD AS "Total Interest Due",
    (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "Crnt Money Due",
    (Tradepieces.MONEY + ABS(TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED) + ABS(TRADEPIECECALCDATAS.REPOINTEREST_NBD)) * TRADETYPES.INTEREST_DIRECTION AS "End Money",
    TRIM(Tradepieces.ISIN) AS "BondID",
    - Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Par/Quantity",
    Tradepiececalcdatas.CURRENTMBSFACTOR AS "Issue Factor",
    - Tradepiececalcdatas.CURRENTMBSFACTOR * Tradepieces.PAR * TRADETYPES.PAR_DIRECTION AS "Current Face",
    TRADEPIECECALCDATAS.CURRENTPRICE AS "Current Price",
    - TRADEPIECECALCDATAS.CURRENTMARKETVALUE * TRADETYPES.PAR_DIRECTION AS "Mkt Value",
    Tradepieces.PRICE AS "Repo Price",
    Tradepieces.PRICEFULL AS "Trade Price",
    Tradepieces.HAIRCUT AS "HairCut",
    CASE
        WHEN Tradepieces.HAIRCUTMETHOD = 0 THEN 'No Haircut'
        WHEN Tradepieces.HAIRCUTMETHOD = 1 THEN '% Proceeds'
        WHEN Tradepieces.HAIRCUTMETHOD = 2 THEN 'Adj By %'
        WHEN Tradepieces.HAIRCUTMETHOD = 3 THEN 'Adj By Points'
        ELSE Tradepieces.X_SPECIALCODE1
    END AS "Haircut Method",
    RTRIM(CASE
        WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
        ELSE ISSUESUBTYPES2.DESCRIPTION
    END) AS "Product Type",
    RTRIM(CASE
        WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD'
        ELSE ISSUESUBTYPES1.DESCRIPTION
    END) AS "Product",
    Tradepieces.USERNAME AS "User",
    RTRIM(ISNULL(Tradepieces.DEPOSITORY, '')) AS "Depository",
    RTRIM(STATUSDETAILS.DESCRIPTION) AS "Status Detail",
    TRIM(Tradepieces.ACCT_NUMBER) AS "CP Short Name",
    RTRIM(STATUSMAINS.DESCRIPTION) AS "Status Main",
    RTRIM(CASE
         WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'Cash'
         WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'Cash'
         ELSE ISSUESUBTYPES3.DESCRIPTION
    END) AS "Product Sub",
    RTRIM(CASE
         WHEN Tradepieces.cusip = 'CASHUSD01' THEN 'USD Cash'
         WHEN Tradepieces.cusip = 'CASHEUR01' THEN 'EUR Cash'
         ELSE ISNULL(ISSUES.DESCRIPTION_1, '')
    END) AS "Description",
    TRADEPIECEXREFS.FRONTOFFICEID AS "Facility",
    Tradepieces.COMMENTS AS "Comments",
    Tradecommissionpieceinfo.commissionvalue AS "Commission",
    ISNULL(Tradepieces.STRATEGY, '') AS "Fund Entity"
FROM
    tradepieces
    INNER JOIN TRADEPIECEXREFS ON TRADEPIECEXREFS.TRADEPIECE = TRADEPIECES.TRADEPIECE
    INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE = TRADEPIECES.TRADEPIECE
    INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE = TRADEPIECES.TRADEPIECE
    INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE = TRADEPIECES.SHELLTRADETYPE
    INNER JOIN ISSUES ON ISSUES.CUSIP = TRADEPIECES.CUSIP
    INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY = TRADEPIECES.CURRENCY_PAR
    INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL = TRADEPIECES.STATUSDETAIL
    INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN = TRADEPIECES.STATUSMAIN
    INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY = TRADEPIECES.ISSUECATEGORY
    INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1 = ISSUECATEGORIES.ISSUESUBTYPE1
    INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2 = ISSUECATEGORIES.ISSUESUBTYPE2
    INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3 = ISSUECATEGORIES.ISSUESUBTYPE3
WHERE
    tradepieces.company IN (45)
    AND tradepieces.statusmain <> 6
    AND ((tradepieces.enddate > DATEADD(DAY, -100, @CustomDate) OR tradepieces.enddate IS NULL)
         AND (tradepieces.closedate IS NULL OR tradepieces.closedate > DATEADD(DAY, -100, @CustomDate)))
    AND Tradepieces.LEDGERNAME = 'Master'
"""

transaction_rec_report_cash_sec_query = """
DECLARE @CustomDate DATE;
SET @CustomDate = {date_placeholder};

SELECT 
    [Local Amount],
    [Settle / Pay Date],
    [Client Reference Number],
    [Transaction Type Name],
    [Status],
    [Reference Number],
    [CUSIP/CINS]
FROM bronze_nexen_cash_and_security_transactions
WHERE [Settle / Pay Date] >= DATEADD(DAY, -4000, @CustomDate);
"""
