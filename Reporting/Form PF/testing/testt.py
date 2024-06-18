"""

Originally created for the Q4-2020 Form PF Filing by Karan Rai

differences between full_output.xml and the submitted form
change DCGXX CUSIP to 262006208 (not X9USDDGCM)
change FILING_TYPE to PF-AMEND (if making multiple versions)
find and replace Other (Government 2a7 Funds) -> Government 2a7 Fund
Change Q53 USG to false

To add a new series, add all of the relevant sheet names to the global constants (SECTION_1B_SHEETS, SECTION_3_SHEETS1, SECTION_3_SHEETS2).
You must also add the Q63 data for the new series to Q63_PATHS
Lastly, you must add the new fund data to FUND_DATA.

ALL OTHER PARAMETERS TO INITIALIZE UP TOP (INCLUDING init_filing() call near top of main)

"""

import xml.etree.ElementTree as ET
import win32com.client as win32
from lxml import etree

# PARAMETERS TO INITIALIZE
WORKBOOK_PATH = "S:\\Mandates\\Funds\\Fund Reporting\\Form PF working files\\curr\\q63_book.xlsx"
XML_OUTPUT_PATH = "full_output.xml"
XSD_PATH = "PFFormFiling.xsd" # one of the JJV additions - used to validate the xml against the xsd schema 
FILING_TYPE = "PF-UPDATING"
FILING_DATE = "2021-09-30" # quarter-end here as YYYY-MM-DD string
FILING_FREQUENCY = "Q"

SECTION_1B_SHEETS = ["Section 1b - Priv Fnd USG M"]
SECTION_3_SHEETS1 = ["Sec 3 Item A-C USG M"]
SECTION_3_SHEETS2 = ["Sec 3 Item D-E USG M"]

Q63_PATHS = ["S:\\Mandates\\Funds\\Fund Reporting\\Form PF working Files\\curr\\q63\\2021_7_8_9_USG_Monthly.xlsx"]


FUND_DATA = [["Lucid Cash Fund USG LLC", "805-6455113436", "LIQUIDITY"]]

NA = -2146826246

root = ET.Element("PFXMLFiling")
excel = win32.gencache.EnsureDispatch('Excel.Application')

def main():
    print("Launching Excel...")
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = open_workbook(excel, WORKBOOK_PATH)
    print("Preparing filing...")
    init_filing(FILING_TYPE, FILING_FREQUENCY, FILING_DATE, "1")
    print("1a_A")
    section1a_ItemA(wb)
    print("1a_B")
    section1a_itemB(wb)
    print("1a_C")
    section1a_itemC(wb)
    print("1b_A")
    section1b_itemA(wb)
    print("1b_B")
    section1b_itemB(wb)
    print("1b_C")
    section1b_itemC(wb)
    #section1c_itemB(wb)
    print("3_ABC")
    section3_itemABC(wb)
    print("3_D")
    section3_itemD(wb)
    print("3_E")
    section3_itemE() # don't need wb for Q63 because data is in seperate wb
    print("3_F")
    section3_itemF(wb)
    print("Exiting Excel...")
    excel.Visible = 1
    excel.Application.Quit()
    wb = None
    excel = None
    tree = ET.ElementTree(root)
    print("Writing XML output to " + XML_OUTPUT_PATH)
    tree.write(XML_OUTPUT_PATH, encoding = "iso-8859-1", xml_declaration=True) #if they encoding type, check if it is utf-8
    print("Validating XML output...")
    try:
        xsd_doc = etree.parse(XSD_PATH)
        xml_schema = etree.XMLSchema(xsd_doc)
        xml_doc = etree.parse(XML_OUTPUT_PATH)
        xml_schema.assertValid(xml_doc)
        print("SUCCESS: File valid.")
    except Exception as e:
        print("FAILED: File not valid.")
        print(e)

def init_filing(filingtype, periodtype, enddate, quarter):

    ET.SubElement(root, "FilingType").text = filingtype
    ET.SubElement(root, "ReportingPeriodType").text = periodtype
    ET.SubElement(root, "ReportingPeriodEndDate").text = enddate
    ET.SubElement(root, "ReportingPeriodQuarter").text = quarter
    ET.SubElement(root, "PFSection1aItemAIdentInfo")

def section1a_ItemA(wb):
    contact = ET.SubElement(root, "PFSection1aItemASignatureFirmAndRelatedPersons")
    ET.SubElement(contact, "IndividualName").text = wb.Worksheets('Item A').Range("C27").Value
    ET.SubElement(contact, "Signature").text = wb.Worksheets('Item A').Range("C28").Value
    ET.SubElement(contact, "Title").text = wb.Worksheets('Item A').Range("C29").Value
    ET.SubElement(contact, "EmailAddress").text = wb.Worksheets('Item A').Range("C30").Value
    ET.SubElement(contact, "TelephoneNumber").text = wb.Worksheets('Item A').Range("C31").Value
    ET.SubElement(contact, "SignatureDate").text = str(wb.Worksheets('Item A').Range("C32").Value).split(" ")[0]

def section1a_itemB(wb):
    sheet = wb.Worksheets('Items B & C')
    q3 = ET.SubElement(root, "PFSection1aItemBFilersAssetsByFundType")
    funds = ET.SubElement(q3, "AssetsFunds")
    liquidityFundData = ET.SubElement(funds, "FilersAssetsFundData")
    ET.SubElement(liquidityFundData, "FundType").text = "LIQUIDITY"
    ET.SubElement(liquidityFundData, "RegulatoryAssetsFunds").text = str(int(sheet.Range("C10").Value))
    ET.SubElement(liquidityFundData, "NetAssetsFunds").text = str(int(sheet.Range("D10").Value))

    privateFundData = ET.SubElement(funds, "FilersAssetsFundData")

    ET.SubElement(privateFundData, "FundType").text = "HEDGE"
    ET.SubElement(privateFundData, "RegulatoryAssetsFunds").text = str(int(sheet.Range("C9").Value))
    ET.SubElement(privateFundData, "NetAssetsFunds").text = str(int(sheet.Range("D9").Value))

def section1a_itemC(wb):
    sheet = wb.Worksheets('Items B & C')
    misc_items = ET.SubElement(root, "PFSection1aItemCMiscellaneousItems")
    misc_item1 = ET.SubElement(misc_items, "PFSection1aItemCMiscellaneous")
    warn = True
    if sheet.Range("B27").Value == "55.(a), 55.(b),55.(f) and 58 ":
        ET.SubElement(misc_item1, "QuestionNumber").text = "55a,b,f; 58"
        warn = False
    else:
        ET.SubElement(misc_item1, "QuestionNumber").text = sheet.Range("B27").Value
    if warn and len(sheet.Range("B27").Value) > 15:
        print("WARNING: Question number response too long. Max length is 15. Value is: " + sheet.Range("B27").Value)
    ET.SubElement(misc_item1, "Description").text =  sheet.Range("C27").Value


    funds = ET.SubElement(root, "PFFunds")

    for data in FUND_DATA:
        fund = ET.SubElement(funds, "PFFund")
        ET.SubElement(fund, "FundID").text = data[1]
        ET.SubElement(fund, "FundType").text = data[2]
        ET.SubElement(fund, "FundName").text = data[0]

def section1b_itemA(wb):
    funds = ET.SubElement(root, "PFSection1bItemAFundsIdentInfo")
    for sheet_name in SECTION_1B_SHEETS:
        sheet = wb.Worksheets(sheet_name)
        fund = ET.SubElement(funds, "PFSection1bItemAFundIdentInfo")
        ET.SubElement(fund, "FundID").text = sheet.Range("C7").Value
        ET.SubElement(fund, "IsMasterFund").text = "false" if sheet.Range("C14").Value == "No" else "true"
        ET.SubElement(fund, "IsParallelFundsAggregated").text = "false" if sheet.Range("C19").Value == "No" else "true"
def section1b_itemB(wb):
    assets_section = ET.SubElement(root, "PFSection1bItemBFundsAssetsFinancingInvestors")
    for sheet_name in SECTION_1B_SHEETS:
        sheet = wb.Worksheets(sheet_name)
        assets = ET.SubElement(assets_section, "PFSection1bItemBFundAssetsFinancingInvestors")
        ET.SubElement(assets, "FundID").text = sheet.Range("C7").Value
        ET.SubElement(assets, "GrossAssetValue").text = str(int(sheet.Range("C35").Value))
        ET.SubElement(assets, "NetAssetValue").text = str(int(sheet.Range("C36").Value))
        ET.SubElement(assets, "EquityValueInOtherFunds").text = str(int(sheet.Range("C39").Value))
        ET.SubElement(assets, "ParallelManagedAccountValue").text = str(int(sheet.Range("C40").Value))

        ET.SubElement(assets, "BorrowedTotalAmount").text = str(int(sheet.Range("C43").Value))

        if sheet.Range("C43").Value != 0 and sheet.Range("C43").Value != "0":
            ET.SubElement(assets, "BorrowedFromUSFinancialInstitutionAmount").text = str(int(sheet.Range("C44").Value))
            ET.SubElement(assets, "BorrowedFromNonUSFinancialInstitutionAmount").text = str(int(sheet.Range("C45").Value))
            ET.SubElement(assets, "BorrowedFromUSNonFinancialInstitutionAmount").text = str(int(sheet.Range("C46").Value))
            ET.SubElement(assets, "BorrowedFromNonUSNonFinancialInstitutionAmount").text = str(int(sheet.Range("C47").Value))

        ET.SubElement(assets, "HasDerivativesPositions").text = "false" if sheet.Range("C50").Value == "No" else "true"
        ET.SubElement(assets, "FairValueLevel1AssetsAmount").text = str(int(sheet.Range("C60").Value))
        ET.SubElement(assets, "FairValueLevel2AssetsAmount").text = str(int(sheet.Range("E60").Value))
        ET.SubElement(assets, "FairValueLevel3AssetsAmount").text = str(int(sheet.Range("F60").Value))
        ET.SubElement(assets, "CostBasedAssetsAmount").text = str(int(sheet.Range("G60").Value))
        ET.SubElement(assets, "FairValueLevel1LiabilitiesAmount").text = str(int(sheet.Range("C61").Value))
        ET.SubElement(assets, "FairValueLevel2LiabilitiesAmount").text = str(int(sheet.Range("E61").Value))
        ET.SubElement(assets, "FairValueLevel3LiabilitiesAmount").text = str(int(sheet.Range("F61").Value))
        ET.SubElement(assets, "CostBasedLiabilitiesAmount").text = str(int(sheet.Range("G61").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByTop5Percent").text = str(int(sheet.Range("C65").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByUSPersonsPercent").text = str(int(sheet.Range("C70").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByNonUSPersonsPercent").text = str(int(sheet.Range("C71").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByBrokerDealersPercent").text = str(int(sheet.Range("C72").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByInsuranceCompaniesPercent").text = str(int(sheet.Range("C73").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByInvestmentCompaniesPercent").text = str(int(sheet.Range("C74").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByPrivateFundsPercent").text = str(int(sheet.Range("C75").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByNonProfitsPercent").text = str(int(sheet.Range("C76").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByPensionPlansPercent").text = str(int(sheet.Range("C77").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByBankingInstitutionsPercent").text = str(int(sheet.Range("C78").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByGovernmentEntitiesPercent").text = str(int(sheet.Range("C79").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByGovernmentPensionsPercent").text = str(int(sheet.Range("C80").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByForeignInstitutionsPercent").text = str(int(sheet.Range("C81").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByNonUSIntermediariesPercent").text = str(int(sheet.Range("C82").Value))
        ET.SubElement(assets, "BeneficiallyOwnedByOthersPercent").text = str(int(sheet.Range("C83").Value))
def section1b_itemC(wb):
    performances = ET.SubElement(root, "PFSection1bItemCFundsPerformance")
    for sheet_name in SECTION_1B_SHEETS:
        sheet = wb.Worksheets(sheet_name)
        performance = ET.SubElement(performances, "PFSection1bItemCFundPerformance")
        ET.SubElement(performance, "FundID").text = sheet.Range("C7").Value
        row_start = 96
        for x in range (1, 13):
            if sheet.Range("C" + str(row_start)).Value == None: # Sec 1b Q17: must leave col C blank if cols E and F aren't populated
                if x % 3 == 0:
                    row_start += 2
                else:
                    row_start += 1
                continue

            date = "LastFiscalDateMonth" + str(x)
            grossValue = "GrossValueMonth" + str(x)
            netValue = "NetValueMonth" + str(x)
            date_elem = ET.SubElement(performance, date)
            ET.SubElement(date_elem, "Value").text = str(sheet.Range("C" + str(row_start)).Value).split(" ")[0]
            gv_elem = ET.SubElement(performance, grossValue)
            ET.SubElement(gv_elem, "Value").text = str(round(sheet.Range("E" + str(row_start)).Value * 100, 2))
            nv_elem = ET.SubElement(performance, netValue)
            ET.SubElement(nv_elem, "Value").text = str(round(sheet.Range("F" + str(row_start)).Value * 100, 2))
            row_start += 1

            if x % 3 == 0:
                dateQ = "LastFiscalDateQuarter" + str(x//3)
                grossQ = "GrossValueQuarter" + str(x//3)
                netValueQ = "NetValueQuarter" + str(x//3)

                date_elem_q = ET.SubElement(performance, dateQ)
                ET.SubElement(date_elem_q, "Value").text = str(sheet.Range("C" + str(row_start)).Value).split(" ")[0]
                gv_elem_q = ET.SubElement(performance, grossQ)
                ET.SubElement(gv_elem_q, "Value").text = str(round(sheet.Range("E" + str(row_start)).Value * 100, 2))
                nv_elem_q = ET.SubElement(performance, netValueQ)
                ET.SubElement(nv_elem_q, "Value").text = str(round(sheet.Range("F" + str(row_start)).Value * 100, 2))
                row_start += 1
        if sheet.Range("C112").Value is not None:
            lfdy = ET.SubElement(performance, "LastFiscalDateYear")
            ET.SubElement(lfdy, "Value").text= str(sheet.Range("C112").Value).split(" ")[0]
            gvy = ET.SubElement(performance, "GrossValueYear")
            ET.SubElement(gvy, "Value").text = str(round(sheet.Range("E112").Value * 100, 2))
            nvy = ET.SubElement(performance, "NetValueYear")
            ET.SubElement(nvy, "Value").text = str(round(sheet.Range("F112").Value * 100, 2))

def section1c_itemB(sheet):
    sheet = wb.Worksheets('Section 1c All Hedge Funds')
    hedgeFunds = ET.SubElement(root, "PFSection1cItemBHedgeFundsInfo")
    hedgeFund = ET.SubElement(hedgeFunds, "PFSection1cItemBHedgeFundInfo")

    ET.SubElement(hedgeFund, "FundID").text = sheet.Range("C6").Value
    ET.SubElement(hedgeFund, "StrategyTypeSM").text = "S" if sheet.Range("C13").Value[0] == 'S' else "M"
    ET.SubElement(hedgeFund, "StrategyHighFrequencyTradingPercent").text = "0"

    stop = ET.SubElement(hedgeFund, "SecuritiesTradedOTCPercent")
    ET.SubElement(stop, "Value").text = str(int(sheet.Range("C86").Value))
    step = ET.SubElement(hedgeFund, "SecuritiesTradedExchangePercent")
    ET.SubElement(step, "Value").text = str(int(sheet.Range("C85").Value))
    dtop = ET.SubElement(hedgeFund, "DerivativesTradedOTCPercent")
    ET.SubElement(dtop, "Value").text = "NA"
    dtep = ET.SubElement(hedgeFund, "DerivativesTradedExchangePercent")
    ET.SubElement(dtep, "Value").text = "NA"
    dtccp = ET.SubElement(hedgeFund, "DerivativesTradedCCPClearedPercent")
    ET.SubElement(dtccp, "Value").text = "NA"
    dtnccp = ET.SubElement(hedgeFund, "DerivativesTradedNotCCPClearedPercent")
    ET.SubElement(dtnccp, "Value").text = "NA"
    rtccp = ET.SubElement(hedgeFund, "RepoTradedCCPClearedPercent")
    ET.SubElement(rtccp, "Value").text = "0" if sheet.Range("C97").Value == '' or sheet.Range("C97").Value is None else str(int(sheet.Range("C97").Value))
    rtnccp = ET.SubElement(hedgeFund, "RepoTradedNotCCPClearedPercent")
    ET.SubElement(rtnccp, "Value").text = "0" if str(int(sheet.Range("C98").Value)) == '' or sheet.Range("C97").Value is None else str(int(sheet.Range("C98").Value))
    rttprp = ET.SubElement(hedgeFund, "RepoTradedTriPartyRepoPercent")
    ET.SubElement(rttprp, "Value").text = "0" if sheet.Range("C99").Value == '' or sheet.Range("C99").Value is None else str(int(sheet.Range("C99").Value))
    ET.SubElement(hedgeFund, "OtherTransactionPercent").text = "0" if sheet.Range("C104").Value == '' or sheet.Range("C104").Value is None else str(int(sheet.Range("C104").Value))

    strat = ET.SubElement(hedgeFund, "Strategies")

    strategy = ET.SubElement(strat, "PFSection1cItemBHedgeFundInfoStrategy")

    ET.SubElement(strategy, "StrategyType").text = "11"
    ET.SubElement(strategy, "StrategyChecked").text = "true"
    ET.SubElement(strategy, "StrategyNAVPercent").text = str(int(sheet.Range("C32").Value))


    cpd = ET.SubElement(hedgeFund, "CounterPartiesDollar")
    for x in range(1,6):
        if sheet.Range("B"+str(x+73)).Value == None:
            break
        cps = ET.SubElement(cpd, "PFSection1cItemBHedgeFundInfoCounterParty")
        ET.SubElement(cps, "CounterpartyType").text = "Dollar"
        ET.SubElement(cps, "CounterpartyLegalName").text = sheet.Range("B"+str(x+73)).Value
        ET.SubElement(cps, "CounterpartyAffiliatedInstitutionCode").text =  "31" if sheet.Range("C"+str(x+73)).Value == "NA" else "27"
        cpe = ET.SubElement(cps, "CounterpartyExposure")
        ET.SubElement(cpe, "Value").text = str(int(sheet.Range("D"+str(x+73)).Value))

def section3_itemABC(wb):

    operations = ET.SubElement(root, "PFSection3ItemALiquidityOperationalInfos")
    assets = ET.SubElement(root, "PFSection3ItemBLiquidityFundsAssets")
    financings = ET.SubElement(root, "PFSection3ItemsCLiquidityFinancingInfo")

    for sheet_name in SECTION_3_SHEETS1:
        sheet = wb.Worksheets(sheet_name)
        # these values ought to be 0 if series undefined that month
        operation = ET.SubElement(operations, "PFSection3ItemALiquidityOperationalInfo")
        ET.SubElement(operation, "FundID").text = sheet.Range("D18").Value
        ET.SubElement(operation, "UseAmortizedCostMethod").text = "false" if sheet.Range("D20").Value == "No" else "true"
        ET.SubElement(operation, "UsePennyRoundingMethod").text = "false" if sheet.Range("D22").Value == "NA" else "true"
        ET.SubElement(operation, "ComplyRule2a7RiskCondition").text = "false" if sheet.Range("D24").Value == "No" else "true"
        ET.SubElement(operation, "ComplyRule2a7DiversificationCondition").text = "false" if sheet.Range("D26").Value == "No" else "true"
        ET.SubElement(operation, "ComplyRule2a7CreditQualityCondition").text = "false" if sheet.Range("D27").Value == "No" else "true"
        ET.SubElement(operation, "ComplyRule2a7LiquidityCondition").text = "false" if sheet.Range("D28").Value == "No" else "true"
        ET.SubElement(operation, "ComplyRule2a7MaturityCondition").text = "false" if sheet.Range("D29").Value == "No" else "true"


        asset = ET.SubElement(assets, "PFSection3ItemBLiquidityAssets")
        ET.SubElement(asset, "FundID").text = sheet.Range("D18").Value
        ET.SubElement(asset, "NAVMonth1Amount").text = str(int(sheet.Range("D36").Value))
        ET.SubElement(asset, "NAVMonth2Amount").text = str(int(sheet.Range("E36").Value))
        ET.SubElement(asset, "NAVMonth3Amount").text = str(int(sheet.Range("F36").Value))
        ET.SubElement(asset, "NAVPerShareMonth1Amount").text = "0" if sheet.Range("D37").Value == "NA" else str(int(sheet.Range("D37").Value))
        ET.SubElement(asset, "NAVPerShareMonth2Amount").text = "0" if sheet.Range("E37").Value == "NA" else str(int(sheet.Range("E37").Value))
        ET.SubElement(asset, "NAVPerShareMonth3Amount").text = "0" if sheet.Range("F37").Value == "NA" else str(int(sheet.Range("F37").Value))
        ET.SubElement(asset, "NAVPerShareMarketBasedMonth1Amount").text = "0" if sheet.Range("D38").Value == "NA" else str(int(sheet.Range("D38").Value))
        ET.SubElement(asset, "NAVPerShareMarketBasedMonth2Amount").text = "0" if sheet.Range("E38").Value == "NA" else str(int(sheet.Range("E38").Value))
        ET.SubElement(asset, "NAVPerShareMarketBasedMonth3Amount").text = "0" if sheet.Range("F38").Value == "NA" else str(int(sheet.Range("F38").Value))

        ET.SubElement(asset, "WAMMonth1Amount").text = str(int(sheet.Range("D39").Value))
        ET.SubElement(asset, "WAMMonth2Amount").text = str(int(sheet.Range("E39").Value))
        
        ET.SubElement(asset, "WAMMonth3Amount").text = str(int(sheet.Range("F39").Value))
        ET.SubElement(asset, "WALMonth1Amount").text = str(int(sheet.Range("D40").Value))
        ET.SubElement(asset, "WALMonth2Amount").text = str(int(sheet.Range("E40").Value))
        ET.SubElement(asset, "WALMonth3Amount").text = str(int(sheet.Range("F40").Value))
        ET.SubElement(asset, "SevenDayGrossYieldMonth1Amount").text = '{0:.2f}'.format(sheet.Range("D41").Value*100)
        ET.SubElement(asset, "SevenDayGrossYieldMonth2Amount").text = '{0:.2f}'.format(sheet.Range("E41").Value*100)
        ET.SubElement(asset, "SevenDayGrossYieldMonth3Amount").text = '{0:.2f}'.format(sheet.Range("F41").Value*100)
        ET.SubElement(asset, "AssetsDailyLiquidMonth1Amount").text = str(int(sheet.Range("D42").Value))
        ET.SubElement(asset, "AssetsDailyLiquidMonth2Amount").text = str(int(sheet.Range("E42").Value))
        ET.SubElement(asset, "AssetsDailyLiquidMonth3Amount").text = str(int(sheet.Range("F42").Value))
        ET.SubElement(asset, "AssetsWeeklyLiquidMonth1Amount").text = str(int(sheet.Range("D43").Value))
        ET.SubElement(asset, "AssetsWeeklyLiquidMonth2Amount").text = str(int(sheet.Range("E43").Value))
        ET.SubElement(asset, "AssetsWeeklyLiquidMonth3Amount").text = str(int(sheet.Range("F43").Value))
        ET.SubElement(asset, "AssetsMaturityGreater397DaysMonth1Amount").text = str(int(sheet.Range("D44").Value))
        ET.SubElement(asset, "AssetsMaturityGreater397DaysMonth2Amount").text = str(int(sheet.Range("E44").Value))
        ET.SubElement(asset, "AssetsMaturityGreater397DaysMonth3Amount").text = str(int(sheet.Range("F44").Value))

        financing = ET.SubElement(financings, "PFSection3ItemCLiquidityFinancingInfo")
        ET.SubElement(financing, "FundID").text = sheet.Range("D18").Value
        ET.SubElement(financing, "BorrowedTotalAmountGreater5Percent").text = "false" if sheet.Range("D50").Value == "No" else "true"
        ET.SubElement(financing, "HasCommittedLiquidityFacilities").text = "false" if sheet.Range("D68").Value == "No" else "true"

def section3_itemD(wb):

    investor_info_list = ET.SubElement(root, "PFSection3ItemDLiquidityInvestorInfoList")

    for sheet_name in SECTION_3_SHEETS2:
        sheet = wb.Worksheets(sheet_name)

        investor_info = ET.SubElement(investor_info_list, "PFSection3ItemDLiquidityInvestorInfo")
        ET.SubElement(investor_info, "FundID").text = sheet.Range("A1").Value
        ET.SubElement(investor_info, "NumberOfOutstandingShares").text = "0" if sheet.Range("D5").Value == "NA" else str(int(sheet.Range("D5").Value))
        ET.SubElement(investor_info, "TopBeneficiallyOwnedEquityPercent").text = str(int(sheet.Range("D9").Value))
        ET.SubElement(investor_info, "NumberOfInvestorsBeneficiallyOwnMore5Percent").text = str(int(sheet.Range("D10").Value))
        ET.SubElement(investor_info, "PercentEquityPurchasedUsingSecuritiesLendingCollateral").text = str(int(sheet.Range("D12").Value))
        ET.SubElement(investor_info, "WithdrawalSuspensionMaybeSubjectedPercent").text = str(int(sheet.Range("D17").Value))
        ET.SubElement(investor_info, "WithdrawalMaterialRestrictionMaybeSubjectedPercent").text = str(int(sheet.Range("D18").Value))
        ET.SubElement(investor_info, "WithdrawalSuspensionIsSubjectedPercent").text = str(int(sheet.Range("D19").Value))
        ET.SubElement(investor_info, "WithdrawalMaterialRestrictionIsSubjectedPercent").text = str(int(sheet.Range("D20").Value))

        ET.SubElement(investor_info, "InvestorLiquidityInDays0To1Percent").text = str(int(sheet.Range("D25").Value))
        ET.SubElement(investor_info, "InvestorLiquidityInDays2To7Percent").text = str(int(sheet.Range("D26").Value))
        ET.SubElement(investor_info, "InvestorLiquidityInDays8To30Percent").text = str(int(sheet.Range("D27").Value))
        ET.SubElement(investor_info, "InvestorLiquidityInDays31To90Percent").text = str(int(sheet.Range("D28").Value))
        ET.SubElement(investor_info, "InvestorLiquidityInDays91To180Percent").text = str(int(sheet.Range("D29").Value))
        ET.SubElement(investor_info, "InvestorLiquidityInDays181To365Percent").text = str(int(sheet.Range("D30").Value))
        ET.SubElement(investor_info, "InvestorLiquidityInDays365MorePercent").text = str(int(sheet.Range("D31").Value))

def section3_itemE():
    """
    Q63
    """

    excel = win32.gencache.EnsureDispatch('Excel.Application')
    infolist = ET.SubElement(root, "PFSection3ItemELiquiditySecurityInfoList")
    count = 0
    for path in Q63_PATHS:
        print("Opening: " + path)
        wb = open_workbook(excel, path)
        sheet = wb.Worksheets('data')

        info = ET.SubElement(infolist, "PFSection3ItemELiquiditySecurityInfo")
        ET.SubElement(info, "FundID").text = FUND_DATA[count][1]
        ET.SubElement(info, "HasNoSecurities").text = "false"
        securitieslist = ET.SubElement(info, "PFSection3ItemELiquiditySecuritiesList")

        index = 3
        while sheet.Range("A" + str(index)).Value is not None:
            security = ET.SubElement(securitieslist, "PFSection3ItemELiquiditySecuritiesItem")
            ET.SubElement(security, "ReportingPeriodMonth").text = str(int(sheet.Range("A" + str(index)).Value))
            ET.SubElement(security, "IssuerName").text = "NA" if sheet.Range("E" + str(index)).Value == NA else sheet.Range("E" + str(index)).Value
            ET.SubElement(security, "IssuerTitle").text = "NA" if sheet.Range("F" + str(index)).Value == NA else sheet.Range("F" + str(index)).Value
            if sheet.Range("G" + str(index)).Value == 262006208:
            	ET.SubElement(security, "CUSIP").text = '262006208'
            else:
            	ET.SubElement(security, "CUSIP").text = "NA" if sheet.Range("G" + str(index)).Value == NA else str(sheet.Range("G" + str(index)).Value)
            ET.SubElement(security, "ISINNumber").text = "NA" if sheet.Range("H" + str(index)).Value == NA else str(sheet.Range("H" + str(index)).Value)
            ET.SubElement(security, "InvestmentCategory").text = "NA" if sheet.Range("I" + str(index)).Value == NA else sheet.Range("I" + str(index)).Value
            if sheet.Range("I" + str(index)).Value == "OTHER":
                ET.SubElement(security, "InvestmentCategoryOther").text = sheet.Range("Z" + str(index)).Value
            ET.SubElement(security, "HasNoRepo").text = "true" if sheet.Range("J" + str(index)).Value == NA else "false"

            if sheet.Range("J" + str(index)).Value != NA:
                ET.SubElement(security, "RepoOpen").text = "false" if sheet.Range("J" + str(index)).Value == NA else str(sheet.Range("J" + str(index)).Value).lower()

                repolist = ET.SubElement(security, "LiquiditySecurityReposList")
                repo = ET.SubElement(repolist, "PFSection3ItemELiquiditySecurityRepo")
                ET.SubElement(repo, "IssuerName").text =  "NA" if sheet.Range("K" + str(index)).Value == NA else sheet.Range("K" + str(index)).Value
                ET.SubElement(repo, "CUSIP").text =  "NA" if sheet.Range("L" + str(index)).Value == NA else str(sheet.Range("L" + str(index)).Value)
                md = ET.SubElement(repo, "MaturityDate")
                ET.SubElement(md, "Value").text =  "NA" if sheet.Range("M" + str(index)).Value == NA or sheet.Range("M" + str(index)).Value is None else str(sheet.Range("M" + str(index)).Value).split(" ")[0]
                ET.SubElement(repo, "Coupon").text =  "NA" if sheet.Range("N" + str(index)).Value == NA else (str(round(sheet.Range("N" + str(index)).Value*100, 5)) + "%")
                pa = ET.SubElement(repo, "PrincipalAmount")
                ET.SubElement(pa, "Value").text =  "NA" if sheet.Range("O" + str(index)).Value == NA or '{0:.2f}'.format(sheet.Range("O" + str(index)).Value) == '0.00' else '{0:.2f}'.format(sheet.Range("O" + str(index)).Value)
                cv = ET.SubElement(repo, "CollateralValue")
                ET.SubElement(cv, "Value").text =  "NA" if sheet.Range("P" + str(index)).Value == NA or '{0:.2f}'.format(sheet.Range("P" + str(index)).Value) == '0.00' else '{0:.2f}'.format(sheet.Range("P" + str(index)).Value)
                ET.SubElement(repo, "Category").text =  "NA" if sheet.Range("Q" + str(index)).Value == NA else sheet.Range("Q" + str(index)).Value
                if sheet.Range("Q" + str(index)).Value == "OTHER":
                    ET.SubElement(repo, "CategoryOtherDesc").text = sheet.Range("AA" + str(index)).Value


            ET.SubElement(security, "HasNoCreditAgency").text = "true"
            wam = ET.SubElement(security, "WAMMaturityDate")
            wal = ET.SubElement(security, "WALMaturityDate")
            ulm = ET.SubElement(security, "UltimateLegalMaturityDate")

            ET.SubElement(wam, "Value").text = "NA" if sheet.Range("R" + str(index)).Value == NA else str(sheet.Range("R" + str(index)).Value).split(" ")[0]
            ET.SubElement(wal, "Value").text = "NA" if sheet.Range("S" + str(index)).Value == NA else str(sheet.Range("S" + str(index)).Value).split(" ")[0]
            ET.SubElement(ulm, "Value").text = "NA" if sheet.Range("T" + str(index)).Value == NA else str(sheet.Range("T" + str(index)).Value).split(" ")[0]
            ET.SubElement(security, "HasNoDemandFeatures").text = "true"
            ET.SubElement(security, "HasNoGuarantee").text = "true"
            ET.SubElement(security, "HasNoEnhancement").text = "true"
            sy = ET.SubElement(security, "SecurityYield")
            ET.SubElement(sy, "Value").text = "NA" if sheet.Range("U" + str(index)).Value == NA else '{0:.2f}'.format(round(sheet.Range("U" + str(index)).Value*100, 2))
            ssv = ET.SubElement(security, "SponsorSupportValue")
            ET.SubElement(ssv, "Value").text = "NA" if sheet.Range("W" + str(index)).Value == NA else '{0:.2f}'.format(round(sheet.Range("W" + str(index)).Value, 2))
            ssav = ET.SubElement(security, "SponsorSupportAmortizedValue")
            ET.SubElement(ssav, "Value").text = "NA" if sheet.Range("W" + str(index)).Value == NA else '{0:.2f}'.format(round(sheet.Range("W" + str(index)).Value, 2))
            ssve = ET.SubElement(security, "SponsorSupportValueExcluded")
            ET.SubElement(ssve, "Value").text = "NA" if sheet.Range("W" + str(index)).Value == NA else '{0:.2f}'.format(round(sheet.Range("W" + str(index)).Value, 2))
            ssave = ET.SubElement(security, "SponsorSupportAmortizedValueExcluded")
            ET.SubElement(ssave, "Value").text = "NA" if sheet.Range("W" + str(index)).Value == NA else '{0:.2f}'.format(round(sheet.Range("W" + str(index)).Value, 2))
            pns = ET.SubElement(security, "PercentNavSecurity")
            ET.SubElement(pns, "Value").text = "NA" if sheet.Range("X" + str(index)).Value == NA else '{0:.2f}'.format(round(sheet.Range("X" + str(index)).Value*100, 2))


            ET.SubElement(security, "IsSecurityAssetOrLiabililty").text = "false"
            ET.SubElement(security, "IsSecurityDailyAsset").text = "true" if sheet.Range("Y" + str(index)).Value == "daily" else "false"
            ET.SubElement(security, "IsSecurityWeeklyAsset").text = "true" if sheet.Range("Y" + str(index)).Value == "weekly" or sheet.Range("Y" + str(index)).Value == "daily" else "false"
            ET.SubElement(security, "IsSecurityIlliquid").text = "false"

            index += 1
        count += 1

def section3_itemF(wb):
    infolist = ET.SubElement(root, "PFSection3ItemFLiquidityParallelMoneyMarketInfoList")

    for sheet_name in SECTION_3_SHEETS2:
        sheet = wb.Worksheets(sheet_name)
        info = ET.SubElement(infolist, "PFSection3ItemFLiquiditySecurityParallelMoneyMarketInfo")
        ET.SubElement(info, "FundID").text = sheet.Range("A1").Value
        ET.SubElement(info, "MMFSeriesNumber").text = sheet.Range("D104").Value

def open_workbook(xlapp, xlfile):
    """
    Helper function to open the workbook.
    Requires an excel instance and a workbook path
    """
    try:
        xlwb = xlapp.Workbooks(xlfile)
    except Exception as e:
        try:
            xlwb = xlapp.Workbooks.Open(xlfile, 0, 1) # don't update links, open read only
        except Exception as e:
            print(e)
            xlwb = None
    return xlwb

def excel_exit():
    excel.Visible = 1
    excel.Application.Quit()
    wb = None
    excel = None

main()