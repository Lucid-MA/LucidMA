OC_query_with_date_constraint = f"""
declare @valdate as date
set @valdate = ?
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
case when tradepieces.startdate <= @valdate then 1 else 0 end 'visible',
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
where (tradepieces.statusmain <> 6) AND ((tradepieces.enddate > @valdate or tradepieces.enddate is null) and (tradepieces.closedate is null or tradepieces.closedate > @valdate))
and tradepieces.company in (44,45)
and (tradetypes.description = 'Reverse' or tradetypes.description = 'ReverseFree' or tradetypes.description = 'RepoFree')
order by tradepieces.company asc, tradepieces.ledgername asc, tradepieces.contraname asc
"""

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

OC_query_test = f"""
declare @valdate as date
set @valdate = '4/01/2024'
declare @fundname as varchar(10)
set @fundname = 'Prime'
declare @seriesname as varchar(15)
set @seriesname = 'Monthly'

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
case when tradepieces.startdate <= @valdate then 1 else 0 end 'visible',
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
where (tradepieces.statusmain <> 6) AND ((tradepieces.enddate > @valdate or tradepieces.enddate is null) and (tradepieces.closedate is null or tradepieces.closedate > @valdate))
and tradepieces.company in (44,45)
and (tradetypes.description = 'Reverse' or tradetypes.description = 'ReverseFree' or tradetypes.description = 'RepoFree')
order by tradepieces.company asc, tradepieces.ledgername asc, tradepieces.contraname asc
"""
