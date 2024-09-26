import itertools
import numpy as np
import pandas as pd
import pymssql
from datetime import date
import tqdm
from datetime import datetime
from numpy import random as rd
from math import exp
from scipy.special import ndtri
from scipy.special import ndtr
from numpy import matmul
from numpy.random import randn
from operator import add 
import warnings
warnings.filterwarnings("error", category=RuntimeWarning) 

def GettingVantage(message, filename):
    print(message)
    result = pd.read_excel(filename, sheet_name='My Table', usecols='B,G,H,J:AA', skiprows=5)
    result.dropna(inplace=True)
    return result

def ImportRawData(DATE):
    ledger_list=['Master','Monthly','Custom1','Quarterly1','MonthlyIG', 'QuarterlyX','Q364','2YIG']
    
    ########################## Excel Imports ##########################
    print('########################## Excel Imports ##########################')
    print('Getting proxy data')
    with pd.ExcelFile(r"S:\Lucid\Investment Committee & Risk\VAR Workspace\proxy.xlsx") as proxy_file:
        proxy = pd.read_excel(proxy_file, usecols='A,C')
    proxy.rename(columns={'Cusip' : 'CUSIP proxy'}, inplace=True)

    print('Getting Bond Data')
    BondData=pd.read_excel(r"S:\Lucid\Data\Bond Data\Bond Data.xlsx",usecols='A,O',skiprows=4)
    BondData.drop([0,1], inplace=True)

    print('Getting ApprovedCounterpartyDatabase')
    ApprovedCouterpartyDatabase=pd.read_excel(r"S:\Lucid\Investment Committee & Risk\Approved Counterparties\Approved CP Database Archive\Approved Counterparty Database_3.22.24.xlsx",sheet_name='Credit & Correlation for IMR',usecols='B,D,H,J',skiprows=5)
    ApprovedCouterpartyDatabase.rename(columns={ApprovedCouterpartyDatabase.columns[1]: 'Grade', ApprovedCouterpartyDatabase.columns[2]: 'Recovery', ApprovedCouterpartyDatabase.columns[3]: 'C/B Corr', }, inplace = True)
    ApprovedCouterpartyDatabase['Counterparty'] = ApprovedCouterpartyDatabase['Counterparty'].str.upper()
    ApprovedCouterpartyDatabase['B/B Corr']= 0.99

    print('Getting AnnualDefaultRates')
    AnnualizedDefaultRates=pd.read_excel(r"S:\Lucid\Investment Committee & Risk\Approved Counterparties\Approved CP Database Archive\Approved Counterparty Database_3.22.24.xlsx",sheet_name='Credit & Correlation for IMR',usecols='S,W',skiprows=6,nrows=18)
    AnnualizedDefaultRates.rename(columns={ AnnualizedDefaultRates.columns[0]:'Grade', AnnualizedDefaultRates.columns[1]: 'AnnualDefaultRate' }, inplace = True)
    AnnualizedDefaultRates.set_index('Grade',inplace=True)

    ApprovedCouterpartyDatabase=ApprovedCouterpartyDatabase.merge(AnnualizedDefaultRates, on='Grade', how='left') 

    VantageUSG = GettingVantage('Getting VantageUSG', r"S:\Lucid\Data\Bond Data\Vantage Data USG.xlsm")
    VantageUSG.drop(index=VantageUSG.index[-1],inplace=True)
    VantageUSG.rename(columns={'Volatility':'Volatility N' , 'Volatility.1':'Volatility S', 'Volatility.2':'Volatility C', 'Px Drop':'Px Drop N', 'Px Drop.1':'Px Drop S', 'Px Drop.2':'Px Drop C', 'Liq Period':'Liq Period N', 'Liq Period.1':'Liq Period S', 'Liq Period.2':'Liq Period C'   }, inplace=True)

    VantagePrime = GettingVantage('Getting VantagePrime',r"S:\Lucid\Data\Bond Data\Vantage Data PRIME.xlsm")
    VantagePrime.rename(columns={'Volatility':'Volatility N' , 'Volatility.1':'Volatility S', 'Volatility.2':'Volatility C', 'Px Drop':'Px Drop N', 'Px Drop.1':'Px Drop S', 'Px Drop.2':'Px Drop C', 'Liq Period':'Liq Period N', 'Liq Period.1':'Liq Period S', 'Liq Period.2':'Liq Period C'   }, inplace=True)
    VantagePrime.drop(index=VantagePrime.index[-1],inplace=True)

    ConcatVantage=pd.concat([VantagePrime,VantageUSG])


    ########################## SQL Imports ##########################
    print('########################## SQL Imports ##########################')
    today=DATE.strftime('%Y-%m-%d')

    print('Getting USG Fund trades')
    connection = pymssql.connect(server='LUCIDSQL1', # type: ignore
                                    user='mattiasalmers',
                                    password='12345',
                                    database='HELIXREPO_PROD_02')
    cursor = connection.cursor()
    query =  '''
        select
    Tradepieces.TRADEPIECE as 'Trade ID',
    trim(Tradepieces.LEDGERNAME) as 'Ledger',
    TRIM(TRADETYPES.DESCRIPTION) as 'TradeType',
    Tradepieces.STARTDATE as 'Start Date',
    CASE WHEN Tradepieces.STATUSMAIN=17 THEN Tradepieces.CLOSEDATE 
    WHEN Tradepieces.CLOSEDATE < Tradepieces.ENDDATE THEN Tradepieces.CLOSEDATE
    ELSE Tradepieces.ENDDATE
    END as 'End Date',
    Tradepieces.FX_MONEY as 'Money',
    trim(Tradepieces.CONTRANAME) as 'Counterparty',
    trim(Tradepieces.ISIN) as 'BondID',
    Tradepieces.PAR as 'Par/Quantity',
    Tradepieces.MBSFACTOR as 'Issue Factor',
    TRADEPIECECALCDATAS.CURRENTPRICE as 'Current Price',
    --,ISNULL(Tradepieces.STRATEGY,'') as 'Fund Entity',
    tradepieces.comments as 'Rating'
    from tradepieces 
    INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE=TRADEPIECES.TRADEPIECE
    INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE=TRADEPIECES.SHELLTRADETYPE
    INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY=TRADEPIECES.ISSUECATEGORY
    left join (
    select distinct history_tradepieces.tradepiece, history_tradepieces.comments rating from history_tradepieces inner join (
    select max(datetimeid) datetimeid, tradepiece from history_tradepieces inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece group by cast(datetimeid as date), tradepiece) maxtbl
    on history_tradepieces.datetimeid = maxtbl.datetimeid and history_tradepieces.tradepiece = maxtbl.tradepiece
    inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece
    where cast(history_tradepieces.datetimeid as date) = cast(history_tradepieces.bookdate as date)
    ) ratings_tbl on ratings_tbl.tradepiece = tradepieces.tradepiece
    where tradepieces.company =  44  AND (tradepieces.statusmain=1 OR tradepieces.statusmain=19 OR tradepieces.statusmain=20 OR tradepieces.statusmain=17) AND (tradepieces.startdate <= CAST(%s as date) and (tradepieces.enddate > CAST(%s as date) OR tradepieces.enddate IS NULL)) and tradepieces.ledgername = 'Monthly' and (tradepieces.closedate is null or tradepieces.closedate <> cast(%s as date)) and not (tradepieces.money <> 0 and tradepieces.cusip = 'CASHUSD01') and trim(Tradepieces.CONTRANAME) <> 'BNYPnI'
    order by tradepieces.contraname asc
    '''
    cursor.execute(query,(today,today,today))
    USG_trades=pd.DataFrame(cursor.fetchall(),columns=['Trade ID','Ledger','TradeType','Start Date','End Date','Money','Counterparty','BondID','Par/Quantity','Issue Factor','Current Price','Rating'])
    USG_trades['Counterparty']=USG_trades['Counterparty'].str.upper()  

    print('Getting Prime Fund trades')
    connection = pymssql.connect(server='LUCIDSQL1', # type: ignore
                                    user='mattiasalmers',
                                    password='12345',
                                    database='HELIXREPO_PROD_02')
    cursor = connection.cursor()
    query =  '''
        select
    Tradepieces.TRADEPIECE as 'Trade ID',
    trim(Tradepieces.LEDGERNAME) as 'Ledger',
    RTRIM(TRADETYPES.DESCRIPTION) as 'TradeType',
    Tradepieces.STARTDATE as 'Start Date',
    CASE WHEN Tradepieces.STATUSMAIN=17 THEN Tradepieces.CLOSEDATE 
    WHEN Tradepieces.CLOSEDATE < Tradepieces.ENDDATE THEN Tradepieces.CLOSEDATE
    ELSE Tradepieces.ENDDATE
    END as 'End Date',
    Tradepieces.FX_MONEY as 'Money',
    trim(Tradepieces.CONTRANAME) as 'Counterparty',
    trim(Tradepieces.ISIN) as 'BondID',
    Tradepieces.PAR as 'Par/Quantity',
    Tradepieces.MBSFACTOR as 'Issue Factor',
    TRADEPIECECALCDATAS.CURRENTPRICE as 'Current Price',
    --,ISNULL(Tradepieces.STRATEGY,'') as 'Fund Entity',
    tradepieces.comments as 'Rating'
    from tradepieces 
    INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE=TRADEPIECES.TRADEPIECE
    INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE=TRADEPIECES.SHELLTRADETYPE
    INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY=TRADEPIECES.ISSUECATEGORY
    left join (
    select distinct history_tradepieces.tradepiece, history_tradepieces.comments rating from history_tradepieces inner join (
    select max(datetimeid) datetimeid, tradepiece from history_tradepieces inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece group by cast(datetimeid as date), tradepiece) maxtbl
    on history_tradepieces.datetimeid = maxtbl.datetimeid and history_tradepieces.tradepiece = maxtbl.tradepiece
    inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece
    where cast(history_tradepieces.datetimeid as date) = cast(history_tradepieces.bookdate as date)
    ) ratings_tbl on ratings_tbl.tradepiece = tradepieces.tradepiece
    where tradepieces.company =  45 AND (tradepieces.statusmain=1 OR tradepieces.statusmain=19 OR tradepieces.statusmain=20 OR tradepieces.statusmain=17) AND (tradepieces.startdate <= CAST(%s as date) and (tradepieces.enddate > CAST(%s as date) OR tradepieces.enddate IS NULL)) and (tradepieces.closedate is null or tradepieces.closedate <> cast(%s as date)) and not (tradepieces.money <> 0 and tradepieces.cusip = 'CASHUSD01')  and trim(Tradepieces.CONTRANAME) <> 'BNYPnI'
    order by tradepieces.contraname asc
    '''
    cursor.execute(query,(today,today,today))
    PRIME_trades_full=pd.DataFrame(cursor.fetchall(),columns=['Trade ID','Ledger','TradeType','Start Date','End Date','Money','Counterparty','BondID','Par/Quantity','Issue Factor','Current Price','Rating'])
    PRIME_trades_full['Counterparty']=PRIME_trades_full['Counterparty'].str.upper()
    PRIME_trades = {
        ledger: PRIME_trades_full.loc[PRIME_trades_full['Ledger'] == ledger]
        for ledger in ledger_list
    }
    BondData = BondData.drop_duplicates(subset=['CUSIP'], keep='last')

    if len(BondData['CUSIP'].unique())!=len(BondData):
        raise ValueError('DUPLICATE IN BOND DATA')

    print('>Data imported with success')

    return {
        'proxy': proxy,
        'BondData': BondData,
        'ApprovedCouterpartyDatabase': ApprovedCouterpartyDatabase,
        'ConcatVantage': ConcatVantage,
        'USG_trades': USG_trades,
        'PRIME_trades': PRIME_trades,
    }

def VAR_param(Data,StressRun,Fund,ledger,DATE):
    proxy=Data.get('proxy')
    BondData=Data.get('BondData')
    ApprovedCouterpartyDatabase=Data.get('ApprovedCouterpartyDatabase')
    ConcatVantage=Data.get('ConcatVantage')
    USG_trades=Data.get('USG_trades')
    PRIME_trades=Data.get('PRIME_trades')


    #Creates Error margin from Price Drop taking into account scenario 
    def ErrorMargin (row):
        if row['TradeType'] in ['ReverseFree', 'RepoFree', 'CashLoan']:
            val=0
        else: 
            val=row['Px Drop '+StressRun]/2
        return val

    # Creates stressed ccollateral value 
    def StressedColl(row):
        if row['TradeType'] in ['RepoFree','Repo']:
            val=0
        else: 
            test=float(row['Par/Quantity']) *(float(row['Current Price']) + float(row['Int Acc']))/100*(1-float(row['Error Margin'])) * float(row['Issue Factor'])
            if np.isnan(test): #if not in BondData, then no Int Acc 
                val=float(row['Par/Quantity']) *float(row['Current Price'])/100*(1-float(row['Error Margin'])) * float(row['Issue Factor'])
            else:
                val=float(row['Par/Quantity']) *(float(row['Current Price']) + float(row['Int Acc']))/100*(1-float(row['Error Margin'])) * float(row['Issue Factor']) 
                
        return val

    # Volatility based on scenario
    def Volatility(row):
        if row['TradeType'] in ['ReverseFree', 'RepoFree', 'CashLoan']:
            val=0
        else: 
            val= row['Volatility '+StressRun]
        return val

    # Liquidation Period (in years) based on scenario
    def Liquidation(row):
        if row['TradeType'] in ['ReverseFree', 'RepoFree', 'CashLoan']:
            val=0
        else: 
            val= row['Liq Period '+StressRun]/365
        return val

    #Counterparties parameters
    if StressRun=='N':
        DefaultRateFactor=1
    elif StressRun=='S':
        DefaultRateFactor=2
    else:
        DefaultRateFactor=3
        
    if Fund=='USG':
        VAR_parameters_USG_counterparties=USG_trades.groupby('Counterparty').agg({'End Date':max})
        VAR_parameters_USG_counterparties['Today']=DATE
        VAR_parameters_USG_counterparties['End Date']=VAR_parameters_USG_counterparties['End Date'].dt.date
        VAR_parameters_USG_counterparties['T']=(VAR_parameters_USG_counterparties['End Date']-VAR_parameters_USG_counterparties['Today']).dt.days
        VAR_parameters_USG_counterparties=VAR_parameters_USG_counterparties.merge(ApprovedCouterpartyDatabase,on='Counterparty', how='left')
        VAR_parameters_USG_counterparties['Default Probability']=1-(1-VAR_parameters_USG_counterparties['AnnualDefaultRate']*DefaultRateFactor)**(VAR_parameters_USG_counterparties['T']/365)
        VAR_parameters_counterparties=VAR_parameters_USG_counterparties
    elif Fund=='Prime': 
        VAR_parameters_PRIME_counterparties=PRIME_trades.get(ledger).groupby('Counterparty').agg({'End Date':max})
        VAR_parameters_PRIME_counterparties['Today']=DATE
        VAR_parameters_PRIME_counterparties['End Date']=VAR_parameters_PRIME_counterparties['End Date'].dt.date
        VAR_parameters_PRIME_counterparties['T'] = (VAR_parameters_PRIME_counterparties['End Date'] - VAR_parameters_PRIME_counterparties['Today']).dt.days
        VAR_parameters_PRIME_counterparties=VAR_parameters_PRIME_counterparties.merge(ApprovedCouterpartyDatabase,on='Counterparty', how='left')
        VAR_parameters_PRIME_counterparties['Default Probability']=1-(1-VAR_parameters_PRIME_counterparties['AnnualDefaultRate']*DefaultRateFactor)**(VAR_parameters_PRIME_counterparties['T']/365)
        VAR_parameters_PRIME_counterparties['Default Probability']=VAR_parameters_PRIME_counterparties['Default Probability'].fillna(0) # if NaN , then all trades of the ocunterparty are in OPEN (cash management), which means the default Probabilioty is null 
        VAR_parameters_counterparties=VAR_parameters_PRIME_counterparties
    else: 
        raise ValueError('No Fund found in VAR param inputs')
    
    #Trades parameters
    if Fund=='USG':
        #Get Vantage CUSIP (without proxy)
        VAR_parameters_USG_trades=USG_trades.merge(ConcatVantage[['CUSIP']], left_on='BondID', right_on='CUSIP', how = 'left')
        VAR_parameters_USG_trades=VAR_parameters_USG_trades.merge(proxy, how = 'left', on='Rating')

        #get Vantage CUSIP with proxy if the Bond ID is not found in Vatange CUSIP
        VAR_parameters_USG_trades['Vantage Bond']=VAR_parameters_USG_trades['CUSIP'].fillna(VAR_parameters_USG_trades['CUSIP proxy'])
        VAR_parameters_USG_trades.drop(columns=['CUSIP', 'CUSIP proxy'], inplace=True)

        #Get Vantage data
        VAR_parameters_USG_trades=VAR_parameters_USG_trades.merge(ConcatVantage, left_on='Vantage Bond', right_on='CUSIP', how='left')
        VAR_parameters_USG_trades.drop(columns=['CUSIP'], inplace=True)

        # get accrual interest from Bond Data
        VAR_parameters_USG_trades=VAR_parameters_USG_trades.merge(BondData, how='left',left_on='BondID', right_on='CUSIP' )

        #Creates Error margin from Price Drop taking into account scenario 
        VAR_parameters_USG_trades['Error Margin']=VAR_parameters_USG_trades.apply(ErrorMargin, axis=1)

        # Creates stressed ccollateral value 
        VAR_parameters_USG_trades['Stressed Collateral Value']=VAR_parameters_USG_trades.apply(StressedColl, axis=1)

        # Volatility based on scenario
        VAR_parameters_USG_trades['Volatility']=VAR_parameters_USG_trades.apply(Volatility, axis=1)

        # Liquidation Period (in years) based on scenario
        VAR_parameters_USG_trades['Liquidation Period']=VAR_parameters_USG_trades.apply(Liquidation, axis=1)

        VAR_parameters_trades=VAR_parameters_USG_trades[['Trade ID','End Date','Money','Counterparty','Volatility','Stressed Collateral Value','Liquidation Period']]
    
    elif Fund=='Prime': 
        VAR_parameters_PRIME_trades=PRIME_trades.get(ledger).merge(ConcatVantage[['CUSIP']], left_on='BondID', right_on='CUSIP', how = 'left')
        VAR_parameters_PRIME_trades=VAR_parameters_PRIME_trades.merge(proxy, how = 'left', on='Rating')

        VAR_parameters_PRIME_trades['Vantage Bond']=VAR_parameters_PRIME_trades['CUSIP'].fillna(VAR_parameters_PRIME_trades['CUSIP proxy'])
        VAR_parameters_PRIME_trades.drop(columns=['CUSIP', 'CUSIP proxy'], inplace=True)

        VAR_parameters_PRIME_trades=VAR_parameters_PRIME_trades.merge(ConcatVantage, left_on='Vantage Bond', right_on='CUSIP', how='left')
        VAR_parameters_PRIME_trades.drop(columns=['CUSIP'], inplace=True)

        VAR_parameters_PRIME_trades=VAR_parameters_PRIME_trades.merge(BondData, how='left',left_on='BondID', right_on='CUSIP' )

        VAR_parameters_PRIME_trades['Error Margin']=VAR_parameters_PRIME_trades.apply(ErrorMargin, axis=1)
        VAR_parameters_PRIME_trades['Stressed Collateral Value']=VAR_parameters_PRIME_trades.apply(StressedColl, axis=1)
        VAR_parameters_PRIME_trades['Volatility']=VAR_parameters_PRIME_trades.apply(Volatility, axis=1)
        VAR_parameters_PRIME_trades['Liquidation Period']=VAR_parameters_PRIME_trades.apply(Liquidation, axis=1)
        VAR_parameters_trades=VAR_parameters_PRIME_trades[['Trade ID','End Date','Money','Counterparty','Volatility','Stressed Collateral Value','Liquidation Period']]
    else: 
        raise ValueError('No Fund found in VAR param inputs')

    VAR_parameters_counterparties['T'].fillna(0, inplace=True) #only margin or on open 
    
    #in case of not existing, to avoid not running the var 
    VAR_parameters_counterparties['Grade'].fillna('BB', inplace=True)
    VAR_parameters_counterparties['Recovery'].fillna(0.25, inplace=True)
    VAR_parameters_counterparties['C/B Corr'].fillna(0.6, inplace=True)
    VAR_parameters_counterparties['B/B Corr'].fillna(0.99, inplace=True)
    VAR_parameters_counterparties['AnnualDefaultRate'].fillna(0.02, inplace=True)
    VAR_parameters_counterparties['Default Probability'].fillna(0.0315395005881266, inplace=True)
    
    #Check of duplicated lines for trades / counterpartiess
    if len(VAR_parameters_trades['Trade ID'].unique())!=len(VAR_parameters_trades):
        raise ValueError('DUPLICATE IN VAR_parameters_trades')
    if len(VAR_parameters_counterparties['Counterparty'].unique())!=len(VAR_parameters_counterparties):
        raise ValueError('DUPLICATE IN VAR_parameters_counterparties')  
    
    return VAR_parameters_counterparties,VAR_parameters_trades

def CholeskyProcess(Data,StressRun,Fund,ledger,DATE):
    #necessary for the multiprocess
    CC_corr=0.5

    VAR_parameters_counterparties,VAR_parameters_trades=VAR_param(Data,StressRun,Fund,ledger,DATE)
    CounterpartyList=VAR_parameters_counterparties['Counterparty']

    #Creates correlation matrix between counterpaties & cholesky decomposition
    K=len(CounterpartyList)
    Counterparty_Correlations=np.full((K,K),CC_corr)
    np.fill_diagonal(Counterparty_Correlations,1)
    L_counterparties=np.linalg.cholesky(Counterparty_Correlations)

    #creates Correlation matrix for each counterparty portfolio in USG
    Trade_Correlations={} #Store Trade correlation matrix of each counterparty
    for counterparty in CounterpartyList:
        temp_trade_list=VAR_parameters_trades.loc[VAR_parameters_trades['Counterparty']==counterparty]
        S=len(temp_trade_list.index) #S number of trades for a single counterparty
        temp_trade_array=np.identity(S+1)
        BB_Corr=VAR_parameters_counterparties.set_index('Counterparty').loc[(counterparty,'B/B Corr')]
        CB_Corr=VAR_parameters_counterparties.set_index('Counterparty').loc[(counterparty,'C/B Corr')]
        for i, j in itertools.product(range(S+1), range(S+1)):
            if i!=j:
                temp_trade_array[i][j] = CB_Corr if i==0 or j==0 else BB_Corr
        Trade_Correlations[counterparty]=temp_trade_array

    L_Trades = {counterparty: np.linalg.cholesky(Trade_Correlations.get(counterparty)) for counterparty in CounterpartyList} # type: ignore
    return VAR_parameters_counterparties,VAR_parameters_trades,L_counterparties, L_Trades

def RunVAR(Results,N,Data,StressRun, Fund, ledger,DATE):
    # sourcery skip: low-code-quality
    print('>>>>>starting New Process for','stressrun=',StressRun,'// Fund=',Fund, '// ledger=', ledger)
    VAR_parameters_counterparties,VAR_parameters_trades,L_counterparties, L_Trades=CholeskyProcess(Data,StressRun,Fund,ledger,DATE)

    CounterpartyList=VAR_parameters_counterparties['Counterparty'].tolist()
    K=len(CounterpartyList)

    Portfolio_Loss_Dic = {counterparty: [] for counterparty in CounterpartyList}

    #OPtimization for the LOOP to avoid pandas calculations
    ProbabilityDefaultList=list(VAR_parameters_counterparties.set_index('Counterparty')['Default Probability'])
    RecoveryRateList=list(VAR_parameters_counterparties.set_index('Counterparty')['Recovery'])
    Single_Counterparty_S_Trades = VAR_parameters_trades['Counterparty'].value_counts().to_dict()
    Single_Counterparty_VAR_trades_parameters = {counterparty: VAR_parameters_trades.loc[VAR_parameters_trades['Counterparty'] == counterparty].set_index('Trade ID') for counterparty in CounterpartyList}

    for _ in range(N):
         
        #Creates correlated random variables bof counterparties probability of default
        #Transform uniform rv into rv to apply cholesky decomposition, creates normal correlated rv and finally trasform in uniform rv
        RV_uncorrelated_Y=np.random.uniform(0, 1, size=(1, K)).tolist()[0]

        RV_uncorrelated_temp_Y = np.apply_along_axis(ndtri, 0, RV_uncorrelated_Y).tolist()
        RV_correlated_temp_Y=matmul(L_counterparties,RV_uncorrelated_temp_Y)

        RV_correlated_Y = np.apply_along_axis(ndtr, 0, RV_correlated_temp_Y).tolist()

        for k, counterparty in enumerate(CounterpartyList):
            Loss=0
            recovery=RecoveryRateList[k]
            PortStrike=0
            PortVal=0

            if RV_correlated_Y[k]<ProbabilityDefaultList[k]: #if default, then calculate rv and lossd for each trade 
                temp_trade=Single_Counterparty_VAR_trades_parameters.get(counterparty)  #Var trade parameters 
                S=Single_Counterparty_S_Trades.get(counterparty) #number of trades

                RV_X_uncorrelated=[ndtri(RV_correlated_Y[k])]+randn(S).tolist() 
                RV_X_correlated=matmul(L_Trades.get(counterparty),RV_X_uncorrelated) # type: ignore

                #value each bond & calculate put payoff (= loss for each trade) 
                trade_list=temp_trade.index.tolist()  

                for s, trade in enumerate(trade_list):
                    S0=float(temp_trade.loc[trade,'Stressed Collateral Value'])
                    sigma=float(temp_trade.loc[trade,'Volatility'])
                    T=float(temp_trade.loc[trade,'Liquidation Period'])
                    Strike=temp_trade.loc[trade,'Money']
                    Xs=RV_X_correlated[s+1]
                    Forward=S0 * exp(  -(sigma**2)*T/2 + sigma*Xs*(T**0.5)  )
                    PortStrike+=Strike 
                    PortVal+=Forward               

            Loss=max(PortStrike-PortVal,0)*(1-recovery)

            #list of all N portfolio losses
            Portfolio_Loss_Dic.get(counterparty).append(Loss)
    
    Portfolio_Loss_Dic['Total FUND'] = [sum(x) for x in zip(*list(Portfolio_Loss_Dic.values()))]

    Portfolio_Loss=VAR_parameters_counterparties[['Counterparty']].copy()
    Portfolio_Loss.set_index('Counterparty', inplace=True)

    # version quicker (almost O(n))
    Portfolio_Loss_Dic_Sorted={}
    for key in Portfolio_Loss_Dic:
        unsorted=Portfolio_Loss_Dic.get(key)
        non_zeros=[x for x in unsorted if x != 0]
        Portfolio_Loss_Dic_Sorted[key] = [0 for _ in range(len(unsorted) - len(non_zeros))] + list(np.sort(non_zeros)) # type: ignore

    #Get the probability of first default for each counterparty    
    for counterparty in CounterpartyList:
        Portfolio_Loss.loc[counterparty,'Probability of First Default']=np.count_nonzero(Portfolio_Loss_Dic_Sorted.get(counterparty))/N # type: ignore

    for counterparty in CounterpartyList:
        Portfolio_Loss.loc[counterparty,'Exp Loss']=int(np.mean(Portfolio_Loss_Dic_Sorted.get(counterparty))) # type: ignore

    #Get the Var for each level
    VARList=[0.8,0.90,0.95,0.98,0.99,0.995,0.999,0.9999,0.99995,0.99999]
    for param in VARList:
        for counterparty in CounterpartyList:
            Portfolio_Loss.loc[counterparty,param]=Portfolio_Loss_Dic_Sorted.get(counterparty)[int(param*N+0.5)]

    #Get the VAR for the Fund
    Portfolio_Loss.loc['Total FUND','Probability of First Default']=np.count_nonzero(Portfolio_Loss_Dic_Sorted.get('Total FUND'))/N # type: ignore
    Portfolio_Loss.loc['Total FUND','Exp Loss']=int(np.mean(Portfolio_Loss_Dic_Sorted.get('Total FUND'))) # type: ignore
    for param in VARList:
        Portfolio_Loss.loc['Total FUND',param]=Portfolio_Loss_Dic_Sorted.get('Total FUND')[int(param*N)]
    
    Portfolio_Loss['Spd Diff'] = ''
    SpdDiff_df=VAR_parameters_trades.groupby('Counterparty').agg({'End Date': 'max', 'Money': 'sum'})
    SpdDiff_df['End Date']= SpdDiff_df['End Date'].dt.date
    for counterparty in CounterpartyList:
        if isinstance(SpdDiff_df.loc[counterparty, 'End Date'], pd._libs.tslibs.nattype.NaTType):
            Portfolio_Loss.loc[counterparty, 'Spd Diff'] = 'OPEN'
        else:
            try:
                Portfolio_Loss.loc[counterparty, 'Spd Diff'] = Portfolio_Loss.loc[counterparty, 'Exp Loss'] / SpdDiff_df.loc[counterparty, 'Money']  * 365 / ((SpdDiff_df.loc[counterparty, 'End Date'] - DATE).days)
            except RuntimeWarning as e:
                Portfolio_Loss.loc[counterparty, 'Spd Diff'] = ['No Money']

    Results[(StressRun,Fund,ledger)]=Portfolio_Loss
   
    print('### PROCESSED  stressrun=',StressRun,'// Fund=',Fund, '// ledger=', ledger,'###')
    return Portfolio_Loss