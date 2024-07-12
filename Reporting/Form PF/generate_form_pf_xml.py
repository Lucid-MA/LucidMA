"""

Originally created for the Q4-2020 Form PF Filing by Karan Rai
Refactored Fall 2021 by JJ Vulopas

Change FILING_TYPE to PF-AMEND (if making multiple versions)
Change Q53 USG to false

To add a new series, add all of the relevant sheet names to the global constants (SECTION_1B_SHEETS, SECTION_3_SHEETS1, SECTION_3_SHEETS2).
Must also add the Q63 data for the new series to Q63_PATHS

Lastly, must add the new fund data to FUND_DATA.

ALL OTHER PARAMETERS TO INITIALIZE UP TOP (INCLUDING init_filing() call near top of main)

"""

import xml.etree.ElementTree as ET
from datetime import datetime

from lxml import etree

from Utils.Common import get_file_path

# CONSTANTS:
# prefix_path = get_file_path("S:/Users/THoang/Tech/LucidMA/Reporting/Form PF/")
prefix_path = get_file_path("S:/Mandates/Funds/Fund Reporting/Form PF working files/")

# PARAMETERS TO INITIALIZE
FILING_DATE = "2024-06-30"  # quarter-end here as YYYY-MM-DD string
IS_QUARTERLY_FILING = False

# Turn this flag on to get data for quarterly only
ONLY_QUARTERLY_DATA = True

DOING_HEDGE = False  # if y/e and doing MMT and other non liq-funds
WORKBOOK_PATH = prefix_path + "2024/07.15.24/Lucid Form PF Q2 - Updated for Part 3 Amendments - Final.xlsx"
XML_OUTPUT_PATH = (
    prefix_path
    + "2024/07.15.24/lucid_form_pf_"
    + datetime.now().strftime("%Y%m%d_%H_%M_%S")
    + ".xml"
)
XSD_PATH = "PFFormFiling_v2.xsd"  # one of the JJV additions - used to validate the xml against the xsd schema
if not ONLY_QUARTERLY_DATA:
    FILING_TYPE = "PF-UPDATING"
else:
    FILING_TYPE = "PF-AMEND"  # one of PF-AMEND, PF-UPDATING, PF-INIT. PF-UPDATING if first one this quarter, else AMEND (INIT only used if first for this firm.)

FILING_FREQUENCY = "Q"

# TODO: ENABLE THIS SECTION ONLY WHEN FINRA REMOVE THE LIMIT OF 16MB PER FILE AND WE CAN FILE BOTH MONTHLY AND QUARTERLY TOGETHER
# SECTION_1B_SHEETS = [
#     "Section 1b - Priv Fnd USG M",
#     "Section 1b - Prv Fnd Prime M",
#     "Section 1b - Prv Fnd Prime C1",
#     "Section 1b - Prv Fnd Prime MIG",
#     # "Section 1b - Prv Fnd Prime Q1",
#     # "Section 1b - Prv Fnd Prime Q364",
#     # "Section 1b - Prv Fnd Prime QX",
# ]
# SECTION_3_SHEETS1 = [
#     "Sec 3 Item A-C USG M",
#     "Sec 3 Item A-C Prime M",
#     "Sec 3 Item A-C Prime C1",
#     "Sec 3 Item A-C Prime MIG",
#     # "Sec 3 Item A-C Prime Q1",
#     # "Sec 3 Item A-C Prime Q364",
#     # "Sec 3 Item A-C Prime QX",
# ]
# SECTION_3_SHEETS2 = [
#     "Sec 3 Item D-E USG M",
#     "Sec 3 Item D-E Prime M",
#     "Sec 3 Item D-E Prime C1",
#     "Sec 3 Item D-E Prime MIG",
#     # "Sec 3 Item D-E Prime Q1",
#     # "Sec 3 Item D-E Prime Q364",
#     # "Sec 3 Item D-E Prime QX",
# ]
# Q58B_SHEETS = {
#     "Sec 3 Item D-E USG M": "Q58-USG M",
#     "Sec 3 Item D-E Prime M": "Q58-Prime M",
#     "Sec 3 Item D-E Prime C1": "Q58-Prime C1",
#     "Sec 3 Item D-E Prime MIG": "Q58-Prime MIG",
#     # "Sec 3 Item D-E Prime Q1": "Q58-Prime Q1",
#     # "Sec 3 Item D-E Prime Q364": "Q58-Prime Q364",
#     # "Sec 3 Item D-E Prime QX": "Q58-Prime QX",
# }
#
# # ORDER MATTERS HERE - Q63_PATHS AND FUND_DATA must be parallel (change line) in section 3E
# # only the liquidity funds for section 3 - can ignore a1, 2yig, mmt
# Q63_PATHS = [
#     prefix_path + "2024/07.15.24/2024_4_5_6_USG_Monthly.xlsx",
#     prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Monthly.xlsx",
#     prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Custom1.xlsx",
#     prefix_path + "2024/07.15.24/2024_4_5_6_Prime_MonthlyIG.xlsx",
#     # prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Quarterly1.xlsx",
#     # prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Q364.xlsx",
#     # prefix_path + "2024/07.15.24/2024_4_5_6_Prime_QuarterlyX.xlsx",
# ]
#
# FUND_DATA = [
#     ["Lucid Cash Fund USG LLC", "805-6455113436", "LIQUIDITY"],
#     ["Lucid Prime Fund LLC [Series M]", "805-2462468395", "LIQUIDITY"],
#     ["Lucid Prime Fund LLC [Series C1]", "805-3531452546", "LIQUIDITY"],
#     ["Lucid Prime Fund LLC [Series MIG]", "805-1061582636", "LIQUIDITY"],
#     # ["Lucid Prime Fund LLC [Series Q1]", "805-2093722753", "LIQUIDITY"],
#     # ["Lucid Prime Fund LLC [Series Q364]", "805-5151206611", "LIQUIDITY"],
#     # ["Lucid Prime Fund LLC [Series QX]", "805-3603861400", "LIQUIDITY"],
# ]

if not ONLY_QUARTERLY_DATA:
    SECTION_1B_SHEETS = [
        "Section 1b - Priv Fnd USG M",
        "Section 1b - Prv Fnd Prime M",
        "Section 1b - Prv Fnd Prime C1",
        "Section 1b - Prv Fnd Prime MIG",
    ]
    SECTION_3_SHEETS1 = [
        "Sec 3 Item A-C USG M",
        "Sec 3 Item A-C Prime M",
        "Sec 3 Item A-C Prime C1",
        "Sec 3 Item A-C Prime MIG",
    ]
    SECTION_3_SHEETS2 = [
        "Sec 3 Item D-E USG M",
        "Sec 3 Item D-E Prime M",
        "Sec 3 Item D-E Prime C1",
        "Sec 3 Item D-E Prime MIG",
    ]
    Q58B_SHEETS = {
        "Sec 3 Item D-E USG M": "Q58-USG M",
        "Sec 3 Item D-E Prime M": "Q58-Prime M",
        "Sec 3 Item D-E Prime C1": "Q58-Prime C1",
        "Sec 3 Item D-E Prime MIG": "Q58-Prime MIG",
    }

    # ORDER MATTERS HERE - Q63_PATHS AND FUND_DATA must be parallel (change line) in section 3E
    # only the liquidity funds for section 3 - can ignore a1, 2yig, mmt
    Q63_PATHS = [
        prefix_path + "2024/07.15.24/2024_4_5_6_USG_Monthly.xlsx",
        prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Monthly.xlsx",
        prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Custom1.xlsx",
        prefix_path + "2024/07.15.24/2024_4_5_6_Prime_MonthlyIG.xlsx",
    ]

    FUND_DATA = [
        ["Lucid Cash Fund USG LLC", "805-6455113436", "LIQUIDITY"],
        ["Lucid Prime Fund LLC [Series M]", "805-2462468395", "LIQUIDITY"],
        ["Lucid Prime Fund LLC [Series C1]", "805-3531452546", "LIQUIDITY"],
        ["Lucid Prime Fund LLC [Series MIG]", "805-1061582636", "LIQUIDITY"],
    ]
else:
    SECTION_1B_SHEETS = [
        "Section 1b - Prv Fnd Prime Q1",
        "Section 1b - Prv Fnd Prime Q364",
        "Section 1b - Prv Fnd Prime QX",
    ]
    SECTION_3_SHEETS1 = [
        "Sec 3 Item A-C Prime Q1",
        "Sec 3 Item A-C Prime Q364",
        "Sec 3 Item A-C Prime QX",
    ]
    SECTION_3_SHEETS2 = [
        "Sec 3 Item D-E Prime Q1",
        "Sec 3 Item D-E Prime Q364",
        "Sec 3 Item D-E Prime QX",
    ]
    Q58B_SHEETS = {
        "Sec 3 Item D-E Prime Q1": "Q58-Prime Q1",
        "Sec 3 Item D-E Prime Q364": "Q58-Prime Q364",
        "Sec 3 Item D-E Prime QX": "Q58-Prime QX",
    }

    # ORDER MATTERS HERE - Q63_PATHS AND FUND_DATA must be parallel (change line) in section 3E
    # only the liquidity funds for section 3 - can ignore a1, 2yig, mmt
    Q63_PATHS = [
        prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Quarterly1.xlsx",
        prefix_path + "2024/07.15.24/2024_4_5_6_Prime_Q364.xlsx",
        prefix_path + "2024/07.15.24/2024_4_5_6_Prime_QuarterlyX.xlsx",
    ]

    FUND_DATA = [
        ["Lucid Prime Fund LLC [Series Q1]", "805-2093722753", "LIQUIDITY"],
        ["Lucid Prime Fund LLC [Series Q364]", "805-5151206611", "LIQUIDITY"],
        ["Lucid Prime Fund LLC [Series QX]", "805-3603861400", "LIQUIDITY"],
    ]


if DOING_HEDGE:
    SECTION_1B_SHEETS.append("Section 1b - Prv Fnd Prime A1")
    SECTION_1B_SHEETS.append("Section 1b - Prv Fnd Prime 2YIG")
    SECTION_1B_SHEETS.append("Section 1b - Prv Fnd MMT T")

if DOING_HEDGE:
    FUND_DATA.append(["Lucid Prime Fund LLC [Series A1]", "805-6101207933", "PRIVATE"])
    FUND_DATA.append(
        ["Lucid Prime Fund LLC [Series 2YIG]", "805-8312373461", "PRIVATE"]
    )
    FUND_DATA.append(["MM Term Income Master Fund LLC", "805-5607102875", "HEDGE"])

NA = -2146826246

root = ET.Element("PFXMLFiling")
# excel = win32.gencache.EnsureDispatch('Excel.Application')

import win32com.client as win32
from win32com.client import gencache

# Use EnsureDispatch to create an Excel instance
# Generate the module explicitly
gencache.EnsureModule("{00020813-0000-0000-C000-000000000046}", 0, 1, 9)
excel = win32.gencache.EnsureDispatch("Excel.Application")


def main():
    print("Launching Excel...")
    # Use EnsureDispatch to create an Excel instance
    excel = win32.gencache.EnsureDispatch("Excel.Application")

    excel.Interactive = False
    wb = open_workbook(excel, WORKBOOK_PATH)
    print("Preparing filing...")
    init_filing(FILING_TYPE, FILING_FREQUENCY, FILING_DATE, "2")
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
    # if DOING_HEDGE:
    #    print("1c_B")
    #    section1c_itemB(wb)
    print("3_ABC")
    section3_itemABC(wb)
    print("3_D")
    section3_itemD(wb)
    print("3_E")
    section3_itemE()  # don't need wb for Q63 because data is in seperate wb
    print("3_F")
    section3_itemF(wb)
    print("3_G")
    section3_itemG(wb)
    print("Exiting Excel...")
    excel.Visible = 1
    excel.Application.Quit()
    wb = None
    excel = None
    tree = ET.ElementTree(root)
    # ET.dump(root) # To write out the output
    print("Writing XML output to " + XML_OUTPUT_PATH)
    tree.write(
        XML_OUTPUT_PATH, encoding="iso-8859-1", xml_declaration=True
    )  # if they encoding type, check if it is utf-8
    print(
        "Validating XML output..."
    )  # one of the JJV additions - validate the xml output against the xsd schema
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
    ET.SubElement(contact, "IndividualName").text = (
        wb.Worksheets("Item A").Range("C27").Value
    )
    ET.SubElement(contact, "Signature").text = (
        wb.Worksheets("Item A").Range("C28").Value
    )
    ET.SubElement(contact, "Title").text = wb.Worksheets("Item A").Range("C29").Value
    ET.SubElement(contact, "EmailAddress").text = (
        wb.Worksheets("Item A").Range("C30").Value
    )
    ET.SubElement(contact, "TelephoneNumber").text = (
        wb.Worksheets("Item A").Range("C31").Value
    )
    ET.SubElement(contact, "SignatureDate").text = str(
        wb.Worksheets("Item A").Range("C32").Value
    ).split(" ")[0]


def section1a_itemB(wb):
    sheet = wb.Worksheets("Items B & C")
    q3 = ET.SubElement(root, "PFSection1aItemBFilersAssetsByFundType")
    funds = ET.SubElement(q3, "AssetsFunds")
    liquidityFundData = ET.SubElement(funds, "FilersAssetsFundData")
    ET.SubElement(liquidityFundData, "FundType").text = "LIQUIDITY"
    ET.SubElement(liquidityFundData, "RegulatoryAssetsFunds").text = str(
        int(sheet.Range("C10").Value)
    )
    ET.SubElement(liquidityFundData, "NetAssetsFunds").text = str(
        int(sheet.Range("D10").Value)
    )

    privateFundData = ET.SubElement(funds, "FilersAssetsFundData")

    ET.SubElement(privateFundData, "FundType").text = "HEDGE"
    ET.SubElement(privateFundData, "RegulatoryAssetsFunds").text = str(
        int(sheet.Range("C9").Value)
    )
    ET.SubElement(privateFundData, "NetAssetsFunds").text = str(
        int(sheet.Range("D9").Value)
    )


def section1a_itemC(wb):
    sheet = wb.Worksheets("Items B & C")
    misc_items = ET.SubElement(root, "PFSection1aItemCMiscellaneousItems")

    # find start
    curr_row = 25
    while curr_row < 50:
        if sheet.Range("B" + str(curr_row)).Value == "Item C. Miscellaneous":
            break
        curr_row = curr_row + 1
    curr_row = curr_row + 6

    q_n = sheet.Range("B" + str(curr_row)).Value
    while q_n != None:
        new_misc_item = ET.SubElement(misc_items, "PFSection1aItemCMiscellaneous")
        warn = True
        if q_n == "55.(a), 55.(b),55.(f) and 58 ":
            ET.SubElement(new_misc_item, "QuestionNumber").text = "55a,b,f; 58"
            warn = False
        else:
            ET.SubElement(new_misc_item, "QuestionNumber").text = "53a,b,f;56;63"
        if warn and len(q_n) > 15:
            print(
                "WARNING: Question number response too long. Max length is 15. Value is: "
                + q_n
            )
        ET.SubElement(new_misc_item, "Description").text = sheet.Range(
            "C" + str(curr_row)
        ).Value
        curr_row = curr_row + 1
        q_n = sheet.Range("B" + str(curr_row)).Value

    funds = ET.SubElement(root, "PFFunds")

    for data in FUND_DATA:
        fund = ET.SubElement(funds, "PFFund")
        ET.SubElement(fund, "FundID").text = data[1]
        ET.SubElement(fund, "FundType").text = data[2]
        ET.SubElement(fund, "FundName").text = data[0]


def section1b_itemA(wb):
    funds = ET.SubElement(root, "PFSection1bItemAFundsIdentInfo")
    for sheet_name in SECTION_1B_SHEETS:
        print(sheet_name)
        sheet = wb.Worksheets(sheet_name)
        fund = ET.SubElement(funds, "PFSection1bItemAFundIdentInfo")
        ET.SubElement(fund, "FundID").text = sheet.Range("C7").Value
        ET.SubElement(fund, "IsMasterFund").text = (
            "false" if sheet.Range("C14").Value == "No" else "true"
        )
        ET.SubElement(fund, "IsParallelFundsAggregated").text = (
            "false" if sheet.Range("C19").Value == "No" else "true"
        )


def section1b_itemB(wb):
    assets_section = ET.SubElement(
        root, "PFSection1bItemBFundsAssetsFinancingInvestors"
    )
    for sheet_name in SECTION_1B_SHEETS:
        sheet = wb.Worksheets(sheet_name)
        assets = ET.SubElement(
            assets_section, "PFSection1bItemBFundAssetsFinancingInvestors"
        )
        ET.SubElement(assets, "FundID").text = sheet.Range("C7").Value
        ET.SubElement(assets, "GrossAssetValue").text = str(
            int(sheet.Range("C35").Value)
        )
        ET.SubElement(assets, "NetAssetValue").text = str(int(sheet.Range("C36").Value))
        ET.SubElement(assets, "EquityValueInOtherFunds").text = str(
            int(sheet.Range("C39").Value)
        )
        ET.SubElement(assets, "ParallelManagedAccountValue").text = str(
            int(sheet.Range("C40").Value)
        )

        ET.SubElement(assets, "BorrowedTotalAmount").text = str(
            int(sheet.Range("C43").Value)
        )

        if sheet.Range("C43").Value != 0 and sheet.Range("C43").Value != "0":
            ET.SubElement(assets, "BorrowedFromUSFinancialInstitutionAmount").text = (
                str(int(sheet.Range("C44").Value))
            )
            ET.SubElement(
                assets, "BorrowedFromNonUSFinancialInstitutionAmount"
            ).text = str(int(sheet.Range("C45").Value))
            ET.SubElement(
                assets, "BorrowedFromUSNonFinancialInstitutionAmount"
            ).text = str(int(sheet.Range("C46").Value))
            ET.SubElement(
                assets, "BorrowedFromNonUSNonFinancialInstitutionAmount"
            ).text = str(int(sheet.Range("C47").Value))

        ET.SubElement(assets, "HasDerivativesPositions").text = (
            "false" if sheet.Range("C50").Value == "No" else "true"
        )
        if IS_QUARTERLY_FILING:
            # DO not need this in quarterly filings
            # Q14 Item B section 1B
            ET.SubElement(assets, "FairValueLevel1AssetsAmount").text = str(
                int(sheet.Range("C60").Value)
            )
            ET.SubElement(assets, "FairValueLevel2AssetsAmount").text = str(
                int(sheet.Range("E60").Value)
            )
            ET.SubElement(assets, "FairValueLevel3AssetsAmount").text = str(
                int(sheet.Range("F60").Value)
            )
            ET.SubElement(assets, "CostBasedAssetsAmount").text = str(
                int(sheet.Range("G60").Value)
            )
            ET.SubElement(assets, "FairValueLevel1LiabilitiesAmount").text = str(
                int(sheet.Range("C61").Value)
            )
            ET.SubElement(assets, "FairValueLevel2LiabilitiesAmount").text = str(
                int(sheet.Range("E61").Value)
            )
            ET.SubElement(assets, "FairValueLevel3LiabilitiesAmount").text = str(
                int(sheet.Range("F61").Value)
            )
            ET.SubElement(assets, "CostBasedLiabilitiesAmount").text = str(
                int(sheet.Range("G61").Value)
            )

        ET.SubElement(assets, "BeneficiallyOwnedByTop5Percent").text = str(
            int(sheet.Range("C65").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByUSPersonsPercent").text = str(
            int(sheet.Range("C70").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByNonUSPersonsPercent").text = str(
            int(sheet.Range("C71").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByBrokerDealersPercent").text = str(
            int(sheet.Range("C72").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByInsuranceCompaniesPercent").text = (
            str(int(sheet.Range("C73").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByInvestmentCompaniesPercent").text = (
            str(int(sheet.Range("C74").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByPrivateFundsPercent").text = str(
            int(sheet.Range("C75").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByNonProfitsPercent").text = str(
            int(sheet.Range("C76").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByPensionPlansPercent").text = str(
            int(sheet.Range("C77").Value)
        )
        ET.SubElement(assets, "BeneficiallyOwnedByBankingInstitutionsPercent").text = (
            str(int(sheet.Range("C78").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByGovernmentEntitiesPercent").text = (
            str(int(sheet.Range("C79").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByGovernmentPensionsPercent").text = (
            str(int(sheet.Range("C80").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByForeignInstitutionsPercent").text = (
            str(int(sheet.Range("C81").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByNonUSIntermediariesPercent").text = (
            str(int(sheet.Range("C82").Value))
        )
        ET.SubElement(assets, "BeneficiallyOwnedByOthersPercent").text = str(
            int(sheet.Range("C83").Value)
        )


def section1b_itemC(wb):
    performances = ET.SubElement(root, "PFSection1bItemCFundsPerformance")
    for sheet_name in SECTION_1B_SHEETS:
        sheet = wb.Worksheets(sheet_name)
        performance = ET.SubElement(performances, "PFSection1bItemCFundPerformance")
        ET.SubElement(performance, "FundID").text = sheet.Range("C7").Value
        row_start = 96
        for x in range(1, 13):
            if (
                sheet.Range("C" + str(row_start)).Value == None
            ):  # Sec 1b Q17: must leave col C blank if cols E and F aren't populated
                if x % 3 == 0:
                    row_start += 2
                else:
                    row_start += 1
                continue

            date = "LastFiscalDateMonth" + str(x)
            grossValue = "GrossValueMonth" + str(x)
            netValue = "NetValueMonth" + str(x)
            date_elem = ET.SubElement(performance, date)
            ET.SubElement(date_elem, "Value").text = str(
                sheet.Range("C" + str(row_start)).Value
            ).split(" ")[0]
            gv_elem = ET.SubElement(performance, grossValue)
            # print(sheet_name, row_start)
            ET.SubElement(gv_elem, "Value").text = str(
                round(sheet.Range("E" + str(row_start)).Value * 100, 2)
            )
            nv_elem = ET.SubElement(performance, netValue)
            ET.SubElement(nv_elem, "Value").text = str(
                round(sheet.Range("F" + str(row_start)).Value * 100, 2)
            )
            row_start += 1

            if x % 3 == 0:
                dateQ = "LastFiscalDateQuarter" + str(x // 3)
                grossQ = "GrossValueQuarter" + str(x // 3)
                netValueQ = "NetValueQuarter" + str(x // 3)

                date_elem_q = ET.SubElement(performance, dateQ)
                ET.SubElement(date_elem_q, "Value").text = str(
                    sheet.Range("C" + str(row_start)).Value
                ).split(" ")[0]
                gv_elem_q = ET.SubElement(performance, grossQ)
                ET.SubElement(gv_elem_q, "Value").text = str(
                    round(sheet.Range("E" + str(row_start)).Value * 100, 2)
                )
                nv_elem_q = ET.SubElement(performance, netValueQ)
                ET.SubElement(nv_elem_q, "Value").text = str(
                    round(sheet.Range("F" + str(row_start)).Value * 100, 2)
                )
                row_start += 1
        if sheet.Range("C112").Value is not None:
            lfdy = ET.SubElement(performance, "LastFiscalDateYear")
            ET.SubElement(lfdy, "Value").text = str(sheet.Range("C112").Value).split(
                " "
            )[0]
            gvy = ET.SubElement(performance, "GrossValueYear")
            ET.SubElement(gvy, "Value").text = str(
                round(sheet.Range("E112").Value * 100, 2)
            )
            nvy = ET.SubElement(performance, "NetValueYear")
            ET.SubElement(nvy, "Value").text = str(
                round(sheet.Range("F112").Value * 100, 2)
            )


def section1c_itemB(wb):
    sheet = wb.Worksheets("Section 1c All Hedge Funds")
    hedgeFunds = ET.SubElement(root, "PFSection1cItemBHedgeFundsInfo")
    hedgeFund = ET.SubElement(hedgeFunds, "PFSection1cItemBHedgeFundInfo")

    ET.SubElement(hedgeFund, "FundID").text = sheet.Range("C6").Value
    ET.SubElement(hedgeFund, "StrategyTypeSM").text = (
        "S" if sheet.Range("C13").Value[0] == "S" else "M"
    )
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
    ET.SubElement(rtccp, "Value").text = (
        "0"
        if sheet.Range("C97").Value == "" or sheet.Range("C97").Value is None
        else str(int(sheet.Range("C97").Value))
    )
    rtnccp = ET.SubElement(hedgeFund, "RepoTradedNotCCPClearedPercent")
    ET.SubElement(rtnccp, "Value").text = (
        "0"
        if str(int(sheet.Range("C98").Value)) == "" or sheet.Range("C97").Value is None
        else str(int(sheet.Range("C98").Value))
    )
    rttprp = ET.SubElement(hedgeFund, "RepoTradedTriPartyRepoPercent")
    ET.SubElement(rttprp, "Value").text = (
        "0"
        if sheet.Range("C99").Value == "" or sheet.Range("C99").Value is None
        else str(int(sheet.Range("C99").Value))
    )
    ET.SubElement(hedgeFund, "OtherTransactionPercent").text = (
        "0"
        if sheet.Range("C104").Value == "" or sheet.Range("C104").Value is None
        else str(int(sheet.Range("C104").Value))
    )

    strat = ET.SubElement(hedgeFund, "Strategies")

    strategy = ET.SubElement(strat, "PFSection1cItemBHedgeFundInfoStrategy")

    ET.SubElement(strategy, "StrategyType").text = "11"
    ET.SubElement(strategy, "StrategyChecked").text = "true"
    ET.SubElement(strategy, "StrategyNAVPercent").text = str(
        int(sheet.Range("C32").Value)
    )

    cpd = ET.SubElement(hedgeFund, "CounterPartiesDollar")
    for x in range(1, 6):
        if sheet.Range("B" + str(x + 73)).Value == None:
            break
        cps = ET.SubElement(cpd, "PFSection1cItemBHedgeFundInfoCounterParty")
        ET.SubElement(cps, "CounterpartyType").text = "Dollar"
        ET.SubElement(cps, "CounterpartyLegalName").text = sheet.Range(
            "B" + str(x + 73)
        ).Value
        ET.SubElement(cps, "CounterpartyAffiliatedInstitutionCode").text = (
            "31"
            if (
                sheet.Range("C" + str(x + 73)).Value == "NA"
                or sheet.Range("C" + str(x + 73)).Value == "N/A"
            )
            else "27"
        )
        cpe = ET.SubElement(cps, "CounterpartyExposure")
        ET.SubElement(cpe, "Value").text = str(
            int(sheet.Range("D" + str(x + 73)).Value)
        )


def section3_itemABC(wb):

    operations = ET.SubElement(root, "PFSection3ItemALiquidityOperationalInfos")
    assets = ET.SubElement(root, "PFSection3ItemBLiquidityFundsAssets")
    financings = ET.SubElement(root, "PFSection3ItemsCLiquidityFinancingInfo")

    for sheet_name in SECTION_3_SHEETS1:
        sheet = wb.Worksheets(sheet_name)
        # these values ought to be 0 if series undefined that month
        operation = ET.SubElement(operations, "PFSection3ItemALiquidityOperationalInfo")
        ET.SubElement(operation, "FundID").text = sheet.Range("D18").Value

        # Q52a
        ET.SubElement(operation, "SeekToMaintainStablePrice").text = (
            "false" if sheet.Range("D20").Value == "No" else "true"
        )

        # Q52b
        # ET.SubElement(operation, "PriceSaughtToMaintain").text = (
        #     "0"
        #     if sheet.Range("D21").Value != "" or sheet.Range("D21").Value is None
        #     else str(int(sheet.Range("D21").Value))
        # )
        if sheet.Range("D21").Value != "" and sheet.Range("D21").Value is not None:
            ET.SubElement(operation, "PriceSaughtToMaintain").text = str(int(sheet.Range("D21").Value))
        # else:
        #     print(f'This is the price saught to maintain value in sheet {sheet_name}', sheet.Range("D21").Value)
        #     ET.SubElement(operation, "PriceSaughtToMaintain").text = '0.00'

        # TODO: Remove question 53
        # ET.SubElement(operation, "ComplyRule2a7RiskCondition").text = (
        #     "false" if sheet.Range("D24").Value == "No" else "true"
        # )
        # TODO: Remove question 54
        # ET.SubElement(operation, "ComplyRule2a7DiversificationCondition").text = (
        #     "false" if sheet.Range("D26").Value == "No" else "true"
        # )
        # ET.SubElement(operation, "ComplyRule2a7CreditQualityCondition").text = (
        #     "false" if sheet.Range("D27").Value == "No" else "true"
        # )
        # ET.SubElement(operation, "ComplyRule2a7LiquidityCondition").text = (
        #     "false" if sheet.Range("D28").Value == "No" else "true"
        # )
        # ET.SubElement(operation, "ComplyRule2a7MaturityCondition").text = (
        #     "false" if sheet.Range("D29").Value == "No" else "true"
        # )

        asset = ET.SubElement(assets, "PFSection3ItemBLiquidityAssets")
        ET.SubElement(asset, "FundID").text = sheet.Range("D18").Value
        # TODO: Update question 55 (now 53)
        ET.SubElement(asset, "NAVMonth1Amount").text = str(
            int(sheet.Range("D28").Value)
        )
        ET.SubElement(asset, "NAVMonth2Amount").text = str(
            int(sheet.Range("E28").Value)
        )
        ET.SubElement(asset, "NAVMonth3Amount").text = str(
            int(sheet.Range("F28").Value)
        )
        ET.SubElement(asset, "NAVPerShareMonth1Amount").text = (
            "0"
            if sheet.Range("D29").Value == "NA"
            else str(int(sheet.Range("D37").Value))
        )
        ET.SubElement(asset, "NAVPerShareMonth2Amount").text = (
            "0"
            if sheet.Range("E29").Value == "NA"
            else str(int(sheet.Range("E29").Value))
        )
        ET.SubElement(asset, "NAVPerShareMonth3Amount").text = (
            "0"
            if sheet.Range("F29").Value == "NA"
            else str(int(sheet.Range("F29").Value))
        )
        ET.SubElement(asset, "NAVPerShareMarketBasedMonth1Amount").text = (
            "0"
            if sheet.Range("D30").Value == "NA"
            else str(int(sheet.Range("D30").Value))
        )
        ET.SubElement(asset, "NAVPerShareMarketBasedMonth2Amount").text = (
            "0"
            if sheet.Range("E30").Value == "NA"
            else str(int(sheet.Range("E30").Value))
        )
        ET.SubElement(asset, "NAVPerShareMarketBasedMonth3Amount").text = (
            "0"
            if sheet.Range("F30").Value == "NA"
            else str(int(sheet.Range("F30").Value))
        )
        ET.SubElement(asset, "WAMMonth1Amount").text = str(
            int(sheet.Range("D31").Value)
        )
        ET.SubElement(asset, "WAMMonth2Amount").text = str(
            int(sheet.Range("E31").Value)
        )

        ET.SubElement(asset, "WAMMonth3Amount").text = str(
            int(sheet.Range("F31").Value)
        )
        ET.SubElement(asset, "WALMonth1Amount").text = str(
            int(sheet.Range("D32").Value)
        )
        ET.SubElement(asset, "WALMonth2Amount").text = str(
            int(sheet.Range("E32").Value)
        )
        ET.SubElement(asset, "WALMonth3Amount").text = str(
            int(sheet.Range("F32").Value)
        )
        ET.SubElement(asset, "SevenDayGrossYieldMonth1Amount").text = "{0:.2f}".format(
            sheet.Range("D33").Value * 100
        )
        ET.SubElement(asset, "SevenDayGrossYieldMonth2Amount").text = "{0:.2f}".format(
            sheet.Range("E33").Value * 100
        )
        ET.SubElement(asset, "SevenDayGrossYieldMonth3Amount").text = "{0:.2f}".format(
            sheet.Range("F33").Value * 100
        )
        ET.SubElement(asset, "AssetsDailyLiquidMonth1Amount").text = str(
            int(sheet.Range("D34").Value)
        )
        ET.SubElement(asset, "AssetsDailyLiquidMonth2Amount").text = str(
            int(sheet.Range("E34").Value)
        )
        ET.SubElement(asset, "AssetsDailyLiquidMonth3Amount").text = str(
            int(sheet.Range("F34").Value)
        )
        ET.SubElement(asset, "AssetsWeeklyLiquidMonth1Amount").text = str(
            int(sheet.Range("D35").Value)
        )
        ET.SubElement(asset, "AssetsWeeklyLiquidMonth2Amount").text = str(
            int(sheet.Range("E35").Value)
        )
        ET.SubElement(asset, "AssetsWeeklyLiquidMonth3Amount").text = str(
            int(sheet.Range("F35").Value)
        )
        ET.SubElement(asset, "AssetsMaturityGreater397DaysMonth1Amount").text = str(
            int(sheet.Range("D36").Value)
        )
        ET.SubElement(asset, "AssetsMaturityGreater397DaysMonth2Amount").text = str(
            int(sheet.Range("E36").Value)
        )
        ET.SubElement(asset, "AssetsMaturityGreater397DaysMonth3Amount").text = str(
            int(sheet.Range("F36").Value)
        )
        ET.SubElement(asset, "CashHeldByFundMonth1Amount").text = str(
            int(sheet.Range("D37").Value)
        )
        ET.SubElement(asset, "CashHeldByFundMonth2Amount").text = str(
            int(sheet.Range("E37").Value)
        )
        ET.SubElement(asset, "CashHeldByFundMonth3Amount").text = str(
            int(sheet.Range("F37").Value)
        )
        ET.SubElement(asset, "TotGrossSubscriptionMonth1Amount").text = str(
            int(sheet.Range("D38").Value)
        )
        ET.SubElement(asset, "TotGrossSubscriptionMonth2Amount").text = str(
            int(sheet.Range("E38").Value)
        )
        ET.SubElement(asset, "TotGrossSubscriptionMonth3Amount").text = str(
            int(sheet.Range("F38").Value)
        )
        ET.SubElement(asset, "TotGrossRedemptionsMonth1Amount").text = str(
            int(sheet.Range("D39").Value)
        )
        ET.SubElement(asset, "TotGrossRedemptionsMonth2Amount").text = str(
            int(sheet.Range("E39").Value)
        )
        ET.SubElement(asset, "TotGrossRedemptionsMonth3Amount").text = str(
            int(sheet.Range("F39").Value)
        )
        financing = ET.SubElement(financings, "PFSection3ItemCLiquidityFinancingInfo")
        ET.SubElement(financing, "FundID").text = sheet.Range("D18").Value
        ET.SubElement(financing, "BorrowedTotalAmountGreater5Percent").text = (
            "false" if sheet.Range("D43").Value == "No" else "true"
        )
        ET.SubElement(financing, "HasCommittedLiquidityFacilities").text = (
            "false" if sheet.Range("D61").Value == "No" else "true"
        )


def is_effectively_zero(value):
    if isinstance(value, str):
        return value in ('0', '0.0', '0.00')
    return isinstance(value, (int, float)) and abs(value) < 1e-10

def is_effectively_zero_or_negative(value):
    if isinstance(value, str):
        return value in ('0', '0.0', '0.00') or value.startswith('-')
    return isinstance(value, (int, float)) and value <= 1e-10

def format_decimal(value, multiply_by_100=False):
    if value == NA or is_effectively_zero_or_negative(value):
        return "NA"
    factor = 100 if multiply_by_100 else 1
    formatted = "{:.2f}".format(round(value * factor, 2))
    return formatted if formatted != '0.00' else "NA"

def section3_itemD(wb):

    investor_info_list = ET.SubElement(root, "PFSection3ItemDLiquidityInvestorInfoList")

    for sheet_name in SECTION_3_SHEETS2:
        sheet = wb.Worksheets(sheet_name)

        investor_info = ET.SubElement(
            investor_info_list, "PFSection3ItemDLiquidityInvestorInfo"
        )
        ET.SubElement(investor_info, "FundID").text = sheet.Range("A1").Value
        ET.SubElement(investor_info, "NumberOfOutstandingShares").text = (
            "0"
            if (sheet.Range("D5").Value == "NA" or sheet.Range("D5").Value == "N/A")
            else str(int(sheet.Range("D5").Value))
        )

        #Q57
        ET.SubElement(investor_info, "IsFundACashManagementVehicle").text = (
            "false" if sheet.Range("D8").Value == "No" else "true"
        )

        # Q58a
        ET.SubElement(investor_info, "TopBeneficiallyOwnedEquityPercent").text = str(
            int(sheet.Range("D12").Value)
        )

        # Q58b
        sub_sheet_name = Q58B_SHEETS[sheet_name]
        sub_sheet = wb.Worksheets(sub_sheet_name)
        starting_row = 2

        # Find the last row where B, C, and D have values
        last_row = starting_row
        while (
                sub_sheet.Range("B" + str(last_row)).Value
                or sub_sheet.Range("C" + str(last_row)).Value
                or sub_sheet.Range("D" + str(last_row)).Value
        ):
            last_row += 1
        last_row -= 1

        if (
            sub_sheet.Range("B2").Value
            and sub_sheet.Range("C2").Value
            and sub_sheet.Range("D2").Value
        ):
            print("Gathering investor data for Question 58b")
            investor_special_list = ET.SubElement(investor_info, "InvestorOwnedPercents")
            for row_idx in range(starting_row, last_row + 1):
                investor_percentage = ET.SubElement(investor_special_list, "PFSection3ItemDLiquidityInvestorPercent")

                ET.SubElement(investor_percentage, "FundInvestorCode").text = str(
                    sub_sheet.Range("D" + str(row_idx)).Value
                )

                # Q62a - Issuer name
                # ET.SubElement(investor_percentage, "PercentageAmount").text = (
                #     "NA"
                #     if sub_sheet.Range("C" + str(row_idx)).Value == NA
                #     else sub_sheet.Range("C" + str(row_idx)).Value
                # )

                percentage_amount = sub_sheet.Range("C" + str(row_idx)).Value
                if percentage_amount == NA or percentage_amount == '':
                    ET.SubElement(investor_percentage, "PercentageAmount").text = "NA"
                else:
                    rounded_amount = round(float(percentage_amount))
                    if rounded_amount > 100:
                        rounded_amount = 100
                    ET.SubElement(investor_percentage, "PercentageAmount").text = str(rounded_amount)
                print(f"Update investor for {sub_sheet_name}: {sub_sheet.Range("B" + str(row_idx)).Value}, {sub_sheet.Range("C" + str(row_idx)).Value}")

        # Q59
        ET.SubElement(
            investor_info, "PercentEquityPurchasedUsingSecuritiesLendingCollateral"
        ).text = str(int(sheet.Range("D20").Value))

        # Q60a-d
        ET.SubElement(
            investor_info, "WithdrawalSuspensionMaybeSubjectedPercent"
        ).text = str(int(sheet.Range("D25").Value))
        ET.SubElement(
            investor_info, "WithdrawalMaterialRestrictionMaybeSubjectedPercent"
        ).text = str(int(sheet.Range("D26").Value))
        ET.SubElement(investor_info, "WithdrawalSuspensionIsSubjectedPercent").text = (
            str(int(sheet.Range("D27").Value))
        )
        ET.SubElement(
            investor_info, "WithdrawalMaterialRestrictionIsSubjectedPercent"
        ).text = str(int(sheet.Range("D28").Value))

        # Q61
        ET.SubElement(investor_info, "InvestorLiquidityInDays0To1Percent").text = str(
            int(sheet.Range("D33").Value)
        )
        ET.SubElement(investor_info, "InvestorLiquidityInDays2To7Percent").text = str(
            int(sheet.Range("D34").Value)
        )
        ET.SubElement(investor_info, "InvestorLiquidityInDays8To30Percent").text = str(
            int(sheet.Range("D35").Value)
        )
        ET.SubElement(investor_info, "InvestorLiquidityInDays31To90Percent").text = str(
            int(sheet.Range("D36").Value)
        )
        ET.SubElement(investor_info, "InvestorLiquidityInDays91To180Percent").text = (
            str(int(sheet.Range("D37").Value))
        )
        ET.SubElement(investor_info, "InvestorLiquidityInDays181To365Percent").text = (
            str(int(sheet.Range("D38").Value))
        )
        ET.SubElement(investor_info, "InvestorLiquidityInDays365MorePercent").text = (
            str(int(sheet.Range("D39").Value))
        )


# TODO: Review changes in this section
def section3_itemE():
    """
    Q62
    """

    excel = win32.gencache.EnsureDispatch("Excel.Application")
    infolist = ET.SubElement(root, "PFSection3ItemELiquiditySecurityInfoList")
    count = 0
    for path in Q63_PATHS:
        print("Opening: " + path)
        wb = open_workbook(excel, path)
        sheet = wb.Worksheets("data")
        info = ET.SubElement(infolist, "PFSection3ItemELiquiditySecurityInfo")
        ET.SubElement(info, "FundID").text = FUND_DATA[count][1]

        if sheet.Range("A3").Value is None:
            ET.SubElement(info, "HasNoSecurities").text = "true"
        else:
            ET.SubElement(info, "HasNoSecurities").text = "false"
            securitieslist = ET.SubElement(
                info, "PFSection3ItemELiquiditySecuritiesList"
            )

            index = 4
            infinite_loop_guard = 500
            max_row = 80000
            while sheet.Range("A" + str(index)).Value is not None and index <= max_row:
                security = ET.SubElement(
                    securitieslist, "PFSection3ItemELiquiditySecuritiesItem"
                )
                ET.SubElement(security, "ReportingPeriodMonth").text = str(
                    int(sheet.Range("A" + str(index)).Value)
                )

                # Q62a - Issuer name
                ET.SubElement(security, "IssuerName").text = (
                    "NA"
                    if sheet.Range("E" + str(index)).Value == NA
                    else sheet.Range("E" + str(index)).Value
                )

                # Q62b - Issuer Title
                ET.SubElement(security, "IssuerTitle").text = (
                    "NA"
                    if sheet.Range("F" + str(index)).Value == NA
                    else sheet.Range("F" + str(index)).Value
                )

                # Q62c - CUSIP
                tmp_cusip = sheet.Range("G" + str(index)).Value
                if tmp_cusip == NA:
                    ET.SubElement(security, "CUSIP").text = "NA"
                else:
                    if tmp_cusip == "X9USDDGCM":
                        tmp_cusip == 262006208
                    try:
                        ET.SubElement(security, "CUSIP").text = str(
                            int(float(tmp_cusip))
                        )  # if all numeric cusip
                        if len(str(int(float(tmp_cusip)))) != 9:
                            print(
                                "WARNING: CUSIP wrong length: "
                                + str(int(float(tmp_cusip)))
                            )
                    except ValueError:
                        ET.SubElement(security, "CUSIP").text = str(tmp_cusip)
                        if len(str(tmp_cusip)) != 9:
                            print("WARNING: CUSIP wrong length: " + str(tmp_cusip))

                # Q62d - LEI
                ET.SubElement(security, "LegalEntityID").text = (
                    "NA"
                    if sheet.Range("H" + str(index)).Value == NA
                       or (
                               sheet.Range("H" + str(index)).Value is not None
                               and len(str(sheet.Range("H" + str(index)).Value)) < 4
                       ) or (sheet.Range("H" + str(index)).Value == "N/A") or  (sheet.Range("H" + str(index)).Value == "Look into")
                    else str(sheet.Range("H" + str(index)).Value)
                )

                # Q62e-i - ISIN
                ET.SubElement(security, "ISINNumber").text = (
                    "NA"
                    if sheet.Range("I" + str(index)).Value == NA
                    else str(sheet.Range("I" + str(index)).Value)
                )

                # Q62f - Investment Category
                investment_category = sheet.Range("J" + str(index)).Value

                ET.SubElement(security, "InvestmentCategory").text = (
                    "NA" if investment_category == NA else
                    "OTHER" if investment_category == "OTHER Government 2a7 Funds" else
                    str(investment_category)
                )
                # Q62f - Investment Category - OTHER
                if sheet.Range("J" + str(index)).Value == "OTHER":
                    ET.SubElement(security, "InvestmentCategoryOther").text = (
                        sheet.Range("AD" + str(index)).Value
                    )

                # Q62g - Has No repo
                ET.SubElement(security, "HasNoRepo").text = (
                    "true" if sheet.Range("K" + str(index)).Value == NA else "false"
                )

                # Q62g-i - Repo Open
                if sheet.Range("K" + str(index)).Value != NA:
                    ET.SubElement(security, "RepoOpen").text = (
                        "false"
                        if sheet.Range("K" + str(index)).Value == NA
                        else str(sheet.Range("K" + str(index)).Value).lower()
                    )

                # Q62g- ii,iii,iv
                ET.SubElement(security, "CentrallyCleared").text = "false"
                # ET.SubElement(security, "CCP").text = "NA"
                ET.SubElement(security, "SettledOnTriPtyPl").text = "false"

                # Q62g- v-xii
                if sheet.Range("K" + str(index)).Value != NA:
                    repolist = ET.SubElement(security, "LiquiditySecurityReposList")
                    repo = ET.SubElement(
                        repolist, "PFSection3ItemELiquiditySecurityRepo"
                    )

                    ET.SubElement(repo, "IssuerName").text = (
                        "NA"
                        if sheet.Range("L" + str(index)).Value == NA
                        else sheet.Range("L" + str(index)).Value
                    )

                    tmp_cusip = sheet.Range("M" + str(index)).Value
                    if tmp_cusip == NA:
                        ET.SubElement(repo, "CUSIP").text = "NA"
                    else:
                        try:
                            ET.SubElement(repo, "CUSIP").text = str(
                                int(float(tmp_cusip))
                            )  # if all numeric cusip
                        except ValueError:
                            ET.SubElement(repo, "CUSIP").text = str(tmp_cusip)

                    # TODO: Might have to include check here
                    ET.SubElement(repo, "LEI").text = (
                        "NA"
                        if sheet.Range("N" + str(index)).Value == NA
                        else sheet.Range("N" + str(index)).Value
                    )

                    md = ET.SubElement(repo, "MaturityDate")
                    tmp_maty = sheet.Range("O" + str(index)).Value
                    if tmp_maty == NA or tmp_maty is None:
                        ET.SubElement(md, "Value").text = "NA"
                    else:
                        tmp_maty = str(tmp_maty).split(" ")[0]
                        try:
                            if float(tmp_maty) <= 1.0:  # if perpetual
                                ET.SubElement(md, "Value").text = "2999-12-31"
                                (
                                    "NOTICE: Perpetual bond detected. "
                                    + tmp_cusip
                                    + " Make note on comments page."
                                )
                        except ValueError:
                            ET.SubElement(md, "Value").text = tmp_maty

                    ET.SubElement(repo, "Coupon").text = (
                        "NA"
                        if sheet.Range("P" + str(index)).Value == NA
                        else (
                            str(round(sheet.Range("P" + str(index)).Value * 100, 5))
                            + "%"
                        )
                    )
                    pa = ET.SubElement(repo, "PrincipalAmount")
                    ET.SubElement(pa, "Value").text = (
                        "NA"
                        if sheet.Range("Q" + str(index)).Value == NA
                        or "{0:.2f}".format(sheet.Range("Q" + str(index)).Value)
                        == "0.00"
                        else "{0:.2f}".format(sheet.Range("Q" + str(index)).Value)
                    )

                    cv = ET.SubElement(repo, "CollateralValue")
                    ET.SubElement(cv, "Value").text = (
                        "NA"
                        if sheet.Range("R" + str(index)).Value == NA
                        or "{0:.2f}".format(sheet.Range("R" + str(index)).Value)
                        == "0.00"
                        else "{0:.2f}".format(sheet.Range("R" + str(index)).Value)
                    )

                    ET.SubElement(repo, "Category").text = (
                        "NA"
                        if sheet.Range("S" + str(index)).Value == NA
                        else sheet.Range("S" + str(index)).Value
                    )

                    if sheet.Range("S" + str(index)).Value == "OTHER":
                        try:
                            other_desc = sheet.Range("AE" + str(index)).Value
                            if other_desc is None or other_desc == NA:
                                ET.SubElement(repo, "CategoryOtherDesc").text = ""
                            else:
                                ET.SubElement(repo, "CategoryOtherDesc").text = str(other_desc)
                        except Exception as e:
                            print(f"Error processing CategoryOtherDesc for row {index}: {e}")
                            ET.SubElement(repo, "CategoryOtherDesc").text = ""
                # TODO: Q62h check w Heather answer is #NA or yes no
                ET.SubElement(security, "HasNoCreditAgency").text = "true"

                wam = ET.SubElement(security, "WAMMaturityDate")
                wal = ET.SubElement(security, "WALMaturityDate")
                ulm = ET.SubElement(security, "UltimateLegalMaturityDate")
                ET.SubElement(wam, "Value").text = (
                    "NA"
                    if sheet.Range("T" + str(index)).Value == NA
                    else str(sheet.Range("T" + str(index)).Value).split(" ")[0]
                )
                ET.SubElement(wal, "Value").text = (
                    "NA"
                    if sheet.Range("U" + str(index)).Value == NA
                    else str(sheet.Range("U" + str(index)).Value).split(" ")[0]
                )
                ET.SubElement(ulm, "Value").text = (
                    "NA"
                    if sheet.Range("V" + str(index)).Value == NA
                    else str(sheet.Range("V" + str(index)).Value).split(" ")[0]
                )
                ET.SubElement(security, "HasNoDemandFeatures").text = "true"
                ET.SubElement(security, "HasNoGuarantee").text = "true"
                ET.SubElement(security, "HasNoEnhancement").text = "true"
                # sy = ET.SubElement(security, "SecurityYield")
                # ET.SubElement(sy, "Value").text = (
                #     "NA"
                #     if sheet.Range("W" + str(index)).Value == NA
                #     else "{0:.2f}".format(
                #         round(sheet.Range("W" + str(index)).Value * 100, 2)
                #     )
                # )
                # ssv = ET.SubElement(security, "SponsorSupportValue")
                # ET.SubElement(ssv, "Value").text = (
                #     "NA"
                #     if sheet.Range("X" + str(index)).Value == NA
                #     else "{0:.2f}".format(round(sheet.Range("X" + str(index)).Value, 2))
                # )
                # ssav = ET.SubElement(security, "SponsorSupportAmortizedValue")
                # ET.SubElement(ssav, "Value").text = (
                #     "NA"
                #     if sheet.Range("Y" + str(index)).Value == NA
                #     else "{0:.2f}".format(round(sheet.Range("Y" + str(index)).Value, 2))
                # )
                # ssve = ET.SubElement(security, "SponsorSupportValueExcluded")
                # ET.SubElement(ssve, "Value").text = (
                #     "NA"
                #     if sheet.Range("Z" + str(index)).Value == NA
                #     else "{0:.2f}".format(round(sheet.Range("Z" + str(index)).Value, 2))
                # )
                # ssave = ET.SubElement(security, "SponsorSupportAmortizedValueExcluded")
                # ET.SubElement(ssave, "Value").text = (
                #     "NA"
                #     if sheet.Range("AA" + str(index)).Value == NA
                #     else "{0:.2f}".format(round(sheet.Range("AA" + str(index)).Value, 2))
                # )

                # SecurityYield
                sy = ET.SubElement(security, "SecurityYield")
                sy_value = sheet.Range("W" + str(index)).Value
                ET.SubElement(sy, "Value").text = format_decimal(sy_value, multiply_by_100=True)

                # SponsorSupportValue
                ssv = ET.SubElement(security, "SponsorSupportValue")
                ssv_value = sheet.Range("X" + str(index)).Value
                ET.SubElement(ssv, "Value").text = format_decimal(ssv_value)

                # SponsorSupportAmortizedValue
                ssav = ET.SubElement(security, "SponsorSupportAmortizedValue")
                ssav_value = sheet.Range("Y" + str(index)).Value
                ET.SubElement(ssav, "Value").text = format_decimal(ssav_value)

                # SponsorSupportValueExcluded
                ssve = ET.SubElement(security, "SponsorSupportValueExcluded")
                ssve_value = sheet.Range("Z" + str(index)).Value
                ET.SubElement(ssve, "Value").text = format_decimal(ssve_value)

                # SponsorSupportValueExcluded
                ssave = ET.SubElement(security, "SponsorSupportAmortizedValueExcluded")
                ssave_value = sheet.Range("AA" + str(index)).Value
                ET.SubElement(ssave, "Value").text = format_decimal(ssave_value)

                pns = ET.SubElement(security, "PercentNavSecurity")
                value = sheet.Range("AB" + str(index)).Value

                if value == NA or is_effectively_zero(value):
                    ET.SubElement(pns, "Value").text = "NA"
                else:
                    formatted_value = "{:.2f}".format(round(value * 100, 2))
                    if formatted_value == '0.00':
                        ET.SubElement(pns, "Value").text = "NA"
                    else:
                        ET.SubElement(pns, "Value").text = formatted_value

                ET.SubElement(security, "IsSecurityAssetOrLiabililty").text = "false"
                ET.SubElement(security, "IsSecurityDailyAsset").text = (
                    "true"
                    if sheet.Range("AC" + str(index)).Value == "daily"
                    else "false"
                )
                ET.SubElement(security, "IsSecurityWeeklyAsset").text = (
                    "true"
                    if sheet.Range("AC" + str(index)).Value == "weekly"
                    or sheet.Range("AC" + str(index)).Value == "daily"
                    else "false"
                )
                ET.SubElement(security, "IsSecurityIlliquid").text = "false"
                if index == infinite_loop_guard:
                    print("On row " + str(index))
                    infinite_loop_guard += 500
                index += 1
        count += 1


def section3_itemF(wb):
    """
    Question 63
    """
    infolist = ET.SubElement(
        root, "PFSection3ItemFDispositionOfPortfolioSecuritiesList"
    )
    for sheet_name in SECTION_3_SHEETS2:
        sheet = wb.Worksheets(sheet_name)
        info = ET.SubElement(
            infolist, "PFSection3ItemFDispositionOfPortfolioSecurities"
        )
        ET.SubElement(info, "FundID").text = sheet.Range("A1").Value
        # # 63a
        # ET.SubElement(info, "USTreasDebtMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D120").Value == "NA"
        #     else str(int(sheet.Range("D120").Value))
        # )
        # ET.SubElement(info, "USTreasDebtMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E120").Value == "NA"
        #     else str(int(sheet.Range("E120").Value))
        # )
        # ET.SubElement(info, "USTreasDebtMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F120").Value == "NA"
        #     else str(int(sheet.Range("F120").Value))
        # )

        if sheet.Range("D120").Value != "NA" and sheet.Range("D120").Value != 0:
            ET.SubElement(info, "USTreasDebtMonth1Amount").text = str(int(sheet.Range("D120").Value))
        if sheet.Range("E120").Value != "NA" and sheet.Range("E120").Value != 0:
            ET.SubElement(info, "USTreasDebtMonth2Amount").text = str(int(sheet.Range("E120").Value))
        if sheet.Range("F120").Value != "NA" and sheet.Range("F120").Value != 0:
            ET.SubElement(info, "USTreasDebtMonth3Amount").text = str(int(sheet.Range("F120").Value))

        # 63b
        # ET.SubElement(info, "USGoveAgcyDebtMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D121").Value == "NA"
        #     else str(int(sheet.Range("D121").Value))
        # )
        # ET.SubElement(info, "USGoveAgcyDebtMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E121").Value == "NA"
        #     else str(int(sheet.Range("E121").Value))
        # )
        # ET.SubElement(info, "USGoveAgcyDebtMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F121").Value == "NA"
        #     else str(int(sheet.Range("F121").Value))
        # )

        if sheet.Range("D121").Value != "NA" and sheet.Range("D121").Value != 0:
            ET.SubElement(info, "USGoveAgcyDebtMonth1Amount").text = str(int(sheet.Range("D121").Value))
        if sheet.Range("E121").Value != "NA" and sheet.Range("E121").Value != 0:
            ET.SubElement(info, "USGoveAgcyDebtMonth2Amount").text = str(int(sheet.Range("E121").Value))
        if sheet.Range("F121").Value != "NA" and sheet.Range("F121").Value != 0:
            ET.SubElement(info, "USGoveAgcyDebtMonth3Amount").text = str(int(sheet.Range("F121").Value))

        # 63c
        # ET.SubElement(info, "USGoveAgcyDebtNoCpnMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D122").Value == "NA"
        #     else str(int(sheet.Range("D122").Value))
        # )
        # ET.SubElement(info, "USGoveAgcyDebtNoCpnMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E122").Value == "NA"
        #     else str(int(sheet.Range("E122").Value))
        # )
        # ET.SubElement(info, "USGoveAgcyDebtNoCpnMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F122").Value == "NA"
        #     else str(int(sheet.Range("F122").Value))
        # )
        if sheet.Range("D122").Value != "NA" and sheet.Range("D122").Value != 0:
            ET.SubElement(info, "USGoveAgcyDebtNoCpnMonth1Amount").text = str(int(sheet.Range("D122").Value))
        if sheet.Range("E122").Value != "NA" and sheet.Range("E122").Value != 0:
            ET.SubElement(info, "USGoveAgcyDebtNoCpnMonth2Amount").text = str(int(sheet.Range("E122").Value))
        if sheet.Range("F122").Value != "NA" and sheet.Range("F122").Value != 0:
            ET.SubElement(info, "USGoveAgcyDebtNoCpnMonth3Amount").text = str(int(sheet.Range("F122").Value))

        # 63d
        # ET.SubElement(info, "NonUsDebtMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D123").Value == "NA"
        #     else str(int(sheet.Range("D123").Value))
        # )
        # ET.SubElement(info, "NonUsDebtMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E123").Value == "NA"
        #     else str(int(sheet.Range("E123").Value))
        # )
        # ET.SubElement(info, "NonUsDebtMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F123").Value == "NA"
        #     else str(int(sheet.Range("F123").Value))
        # )
        if sheet.Range("D123").Value != "NA" and sheet.Range("D123").Value != 0:
            ET.SubElement(info, "NonUsDebtMonth1Amount").text = str(int(sheet.Range("D123").Value))
        if sheet.Range("E123").Value != "NA" and sheet.Range("E123").Value != 0:
            ET.SubElement(info, "NonUsDebtMonth2Amount").text = str(int(sheet.Range("E123").Value))
        if sheet.Range("F123").Value != "NA" and sheet.Range("F123").Value != 0:
            ET.SubElement(info, "NonUsDebtMonth3Amount").text = str(int(sheet.Range("F123").Value))

        # 63e
        # ET.SubElement(info, "CertOfDepositMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D124").Value == "NA"
        #     else str(int(sheet.Range("D124").Value))
        # )
        # ET.SubElement(info, "CertOfDepositMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E124").Value == "NA"
        #     else str(int(sheet.Range("E124").Value))
        # )
        # ET.SubElement(info, "CertOfDepositMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F124").Value == "NA"
        #     else str(int(sheet.Range("F124").Value))
        # )
        if sheet.Range("D124").Value != "NA" and sheet.Range("D124").Value != 0:
            ET.SubElement(info, "CertOfDepositMonth1Amount").text = str(int(sheet.Range("D124").Value))
        if sheet.Range("E124").Value != "NA" and sheet.Range("E124").Value != 0:
            ET.SubElement(info, "CertOfDepositMonth2Amount").text = str(int(sheet.Range("E124").Value))
        if sheet.Range("F124").Value != "NA" and sheet.Range("F124").Value != 0:
            ET.SubElement(info, "CertOfDepositMonth3Amount").text = str(int(sheet.Range("F124").Value))

        # 63f
        # ET.SubElement(info, "NonNegTimeDepositMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D125").Value == "NA"
        #     else str(int(sheet.Range("D125").Value))
        # )
        # ET.SubElement(info, "NonNegTimeDepositMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E125").Value == "NA"
        #     else str(int(sheet.Range("E125").Value))
        # )
        # ET.SubElement(info, "NonNegTimeDepositMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F125").Value == "NA"
        #     else str(int(sheet.Range("F125").Value))
        # )
        if sheet.Range("D125").Value != "NA" and sheet.Range("D125").Value != 0:
            ET.SubElement(info, "NonNegTimeDepositMonth1Amount").text = str(int(sheet.Range("D125").Value))
        if sheet.Range("E125").Value != "NA" and sheet.Range("E125").Value != 0:
            ET.SubElement(info, "NonNegTimeDepositMonth2Amount").text = str(int(sheet.Range("E125").Value))
        if sheet.Range("F125").Value != "NA" and sheet.Range("F125").Value != 0:
            ET.SubElement(info, "NonNegTimeDepositMonth3Amount").text = str(int(sheet.Range("F125").Value))

        # # 63g
        # ET.SubElement(info, "VarRateDemandNoteMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D126").Value == "NA"
        #     else str(int(sheet.Range("D126").Value))
        # )
        # ET.SubElement(info, "VarRateDemandNoteMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E126").Value == "NA"
        #     else str(int(sheet.Range("E126").Value))
        # )
        # ET.SubElement(info, "VarRateDemandNoteMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F126").Value == "NA"
        #     else str(int(sheet.Range("F126").Value))
        # )
        #
        # # 63h
        # ET.SubElement(info, "OtherMncplSecurityMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D127").Value == "NA"
        #     else str(int(sheet.Range("D127").Value))
        # )
        # ET.SubElement(info, "OtherMncplSecurityMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E127").Value == "NA"
        #     else str(int(sheet.Range("E127").Value))
        # )
        # ET.SubElement(info, "OtherMncplSecurityMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F127").Value == "NA"
        #     else str(int(sheet.Range("F127").Value))
        # )
        #
        # # 63i
        # ET.SubElement(info, "AssetBackedCommercialPaperMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D128").Value == "NA"
        #     else str(int(sheet.Range("D128").Value))
        # )
        # ET.SubElement(info, "AssetBackedCommercialPaperMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E128").Value == "NA"
        #     else str(int(sheet.Range("E128").Value))
        # )
        # ET.SubElement(info, "AssetBackedCommercialPaperMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F128").Value == "NA"
        #     else str(int(sheet.Range("F128").Value))
        # )
        #
        # # 63j
        # ET.SubElement(info, "OtherAssetBackedSecuritiesMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D129").Value == "NA"
        #     else str(int(sheet.Range("D129").Value))
        # )
        # ET.SubElement(info, "OtherAssetBackedSecuritiesMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E129").Value == "NA"
        #     else str(int(sheet.Range("E129").Value))
        # )
        # ET.SubElement(info, "OtherAssetBackedSecuritiesMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F129").Value == "NA"
        #     else str(int(sheet.Range("F129").Value))
        # )
        #
        # # 63k
        # ET.SubElement(info, "USTreasuryRepoMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D130").Value == "NA"
        #     else str(int(sheet.Range("D130").Value))
        # )
        # ET.SubElement(info, "USTreasuryRepoMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E130").Value == "NA"
        #     else str(int(sheet.Range("E130").Value))
        # )
        # ET.SubElement(info, "USTreasuryRepoMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F130").Value == "NA"
        #     else str(int(sheet.Range("F130").Value))
        # )
        #
        # # 63l
        # ET.SubElement(info, "USGovAgencyRepoMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D131").Value == "NA"
        #     else str(int(sheet.Range("D131").Value))
        # )
        # ET.SubElement(info, "USGovAgencyRepoMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E131").Value == "NA"
        #     else str(int(sheet.Range("E131").Value))
        # )
        # ET.SubElement(info, "USGovAgencyRepoMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F131").Value == "NA"
        #     else str(int(sheet.Range("F131").Value))
        # )
        #
        # # 63m
        # ET.SubElement(info, "OtherRepoMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D132").Value == "NA"
        #     else str(int(sheet.Range("D132").Value))
        # )
        # ET.SubElement(info, "OtherRepoMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E132").Value == "NA"
        #     else str(int(sheet.Range("E132").Value))
        # )
        # ET.SubElement(info, "OtherRepoMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F132").Value == "NA"
        #     else str(int(sheet.Range("F132").Value))
        # )
        #
        # # 63n
        # ET.SubElement(info, "InsuranceCoFundingAgrmntMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D133").Value == "NA"
        #     else str(int(sheet.Range("D133").Value))
        # )
        # ET.SubElement(info, "InsuranceCoFundingAgrmntMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E133").Value == "NA"
        #     else str(int(sheet.Range("E133").Value))
        # )
        # ET.SubElement(info, "InsuranceCoFundingAgrmntMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F133").Value == "NA"
        #     else str(int(sheet.Range("F133").Value))
        # )
        #
        # # 63o
        # ET.SubElement(info, "InvestmentCompanyMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D134").Value == "NA"
        #     else str(int(sheet.Range("D120").Value))
        # )
        # ET.SubElement(info, "InvestmentCompanyMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E134").Value == "NA"
        #     else str(int(sheet.Range("E134").Value))
        # )
        # ET.SubElement(info, "InvestmentCompanyMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F134").Value == "NA"
        #     else str(int(sheet.Range("F134").Value))
        # )
        #
        # # 63p
        # ET.SubElement(info, "FinancialCoCommercialPaperMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D135").Value == "NA"
        #     else str(int(sheet.Range("D135").Value))
        # )
        # ET.SubElement(info, "FinancialCoCommercialPaperMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E135").Value == "NA"
        #     else str(int(sheet.Range("E135").Value))
        # )
        # ET.SubElement(info, "FinancialCoCommercialPaperMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F135").Value == "NA"
        #     else str(int(sheet.Range("F135").Value))
        # )
        #
        # # 63q
        # ET.SubElement(info, "NonFinancialCoCommercialPaperMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D136").Value == "NA"
        #     else str(int(sheet.Range("D136").Value))
        # )
        # ET.SubElement(info, "NonFinancialCoCommercialPaperMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E136").Value == "NA"
        #     else str(int(sheet.Range("E136").Value))
        # )
        # ET.SubElement(info, "NonFinancialCoCommercialPaperMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F136").Value == "NA"
        #     else str(int(sheet.Range("F136").Value))
        # )
        #
        # # 63r
        # ET.SubElement(info, "TenderOptionBondMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D137").Value == "NA"
        #     else str(int(sheet.Range("D137").Value))
        # )
        # ET.SubElement(info, "TenderOptionBondMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E137").Value == "NA"
        #     else str(int(sheet.Range("E137").Value))
        # )
        # ET.SubElement(info, "TenderOptionBondMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F137").Value == "NA"
        #     else str(int(sheet.Range("F137").Value))
        # )
        # # 63s
        # ET.SubElement(info, "OtherInstrumentMonth1Amount").text = (
        #     "0"
        #     if sheet.Range("D138").Value == "NA"
        #     else str(int(sheet.Range("D138").Value))
        # )
        # ET.SubElement(info, "OtherInstrumentMonth2Amount").text = (
        #     "0"
        #     if sheet.Range("E138").Value == "NA"
        #     else str(int(sheet.Range("E138").Value))
        # )
        # ET.SubElement(info, "OtherInstrumentMonth3Amount").text = (
        #     "0"
        #     if sheet.Range("F138").Value == "NA"
        #     else str(int(sheet.Range("F138").Value))
        # )
        #
        #
        # 63g
        if sheet.Range("D126").Value != "NA" and sheet.Range("D126").Value != 0:
            ET.SubElement(info, "VarRateDemandNoteMonth1Amount").text = str(int(sheet.Range("D126").Value))
        if sheet.Range("E126").Value != "NA" and sheet.Range("E126").Value != 0:
            ET.SubElement(info, "VarRateDemandNoteMonth2Amount").text = str(int(sheet.Range("E126").Value))
        if sheet.Range("F126").Value != "NA" and sheet.Range("F126").Value != 0:
            ET.SubElement(info, "VarRateDemandNoteMonth3Amount").text = str(int(sheet.Range("F126").Value))

        # 63h
        if sheet.Range("D127").Value != "NA" and sheet.Range("D127").Value != 0:
            ET.SubElement(info, "OtherMncplSecurityMonth1Amount").text = str(int(sheet.Range("D127").Value))
        if sheet.Range("E127").Value != "NA" and sheet.Range("E127").Value != 0:
            ET.SubElement(info, "OtherMncplSecurityMonth2Amount").text = str(int(sheet.Range("E127").Value))
        if sheet.Range("F127").Value != "NA" and sheet.Range("F127").Value != 0:
            ET.SubElement(info, "OtherMncplSecurityMonth3Amount").text = str(int(sheet.Range("F127").Value))

        # 63i
        if sheet.Range("D128").Value != "NA" and sheet.Range("D128").Value != 0:
            ET.SubElement(info, "AssetBackedCommercialPaperMonth1Amount").text = str(int(sheet.Range("D128").Value))
        if sheet.Range("E128").Value != "NA" and sheet.Range("E128").Value != 0:
            ET.SubElement(info, "AssetBackedCommercialPaperMonth2Amount").text = str(int(sheet.Range("E128").Value))
        if sheet.Range("F128").Value != "NA" and sheet.Range("F128").Value != 0:
            ET.SubElement(info, "AssetBackedCommercialPaperMonth3Amount").text = str(int(sheet.Range("F128").Value))

        # 63j
        if sheet.Range("D129").Value != "NA" and sheet.Range("D129").Value != 0:
            ET.SubElement(info, "OtherAssetBackedSecuritiesMonth1Amount").text = str(int(sheet.Range("D129").Value))
        if sheet.Range("E129").Value != "NA" and sheet.Range("E129").Value != 0:
            ET.SubElement(info, "OtherAssetBackedSecuritiesMonth2Amount").text = str(int(sheet.Range("E129").Value))
        if sheet.Range("F129").Value != "NA" and sheet.Range("F129").Value != 0:
            ET.SubElement(info, "OtherAssetBackedSecuritiesMonth3Amount").text = str(int(sheet.Range("F129").Value))

        # 63k
        if sheet.Range("D130").Value != "NA" and sheet.Range("D130").Value != 0:
            ET.SubElement(info, "USTreasuryRepoMonth1Amount").text = str(int(sheet.Range("D130").Value))
        if sheet.Range("E130").Value != "NA" and sheet.Range("E130").Value != 0:
            ET.SubElement(info, "USTreasuryRepoMonth2Amount").text = str(int(sheet.Range("E130").Value))
        if sheet.Range("F130").Value != "NA" and sheet.Range("F130").Value != 0:
            ET.SubElement(info, "USTreasuryRepoMonth3Amount").text = str(int(sheet.Range("F130").Value))

        # 63l
        if sheet.Range("D131").Value != "NA" and sheet.Range("D131").Value != 0 or (sheet.Range("E131").Value != "NA" and sheet.Range("E131").Value != 0) or (sheet.Range("F131").Value != "NA" and sheet.Range("F131").Value != 0):
            ET.SubElement(info, "USGovAgencyRepoMonth1Amount").text = str(int(sheet.Range("D131").Value))
            ET.SubElement(info, "USGovAgencyRepoMonth2Amount").text = str(int(sheet.Range("E131").Value))
            ET.SubElement(info, "USGovAgencyRepoMonth3Amount").text = str(int(sheet.Range("F131").Value))

        # 63m
        if sheet.Range("D132").Value != "NA" and sheet.Range("D132").Value != 0:
            ET.SubElement(info, "OtherRepoMonth1Amount").text = str(int(sheet.Range("D132").Value))
        if sheet.Range("E132").Value != "NA" and sheet.Range("E132").Value != 0:
            ET.SubElement(info, "OtherRepoMonth2Amount").text = str(int(sheet.Range("E132").Value))
        if sheet.Range("F132").Value != "NA" and sheet.Range("F132").Value != 0:
            ET.SubElement(info, "OtherRepoMonth3Amount").text = str(int(sheet.Range("F132").Value))

        # 63n
        if sheet.Range("D133").Value != "NA" and sheet.Range("D133").Value != 0:
            ET.SubElement(info, "InsuranceCoFundingAgrmntMonth1Amount").text = str(int(sheet.Range("D133").Value))
        if sheet.Range("E133").Value != "NA" and sheet.Range("E133").Value != 0:
            ET.SubElement(info, "InsuranceCoFundingAgrmntMonth2Amount").text = str(int(sheet.Range("E133").Value))
        if sheet.Range("F133").Value != "NA" and sheet.Range("F133").Value != 0:
            ET.SubElement(info, "InsuranceCoFundingAgrmntMonth3Amount").text = str(int(sheet.Range("F133").Value))

        # 63o
        if sheet.Range("D134").Value != "NA" and sheet.Range("D134").Value != 0:
            ET.SubElement(info, "InvestmentCompanyMonth1Amount").text = str(int(sheet.Range("D120").Value))
        if sheet.Range("E134").Value != "NA" and sheet.Range("E134").Value != 0:
            ET.SubElement(info, "InvestmentCompanyMonth2Amount").text = str(int(sheet.Range("E134").Value))
        if sheet.Range("F134").Value != "NA" and sheet.Range("F134").Value != 0:
            ET.SubElement(info, "InvestmentCompanyMonth3Amount").text = str(int(sheet.Range("F134").Value))

        # 63p
        if sheet.Range("D135").Value != "NA" and sheet.Range("D135").Value != 0:
            ET.SubElement(info, "FinancialCoCommercialPaperMonth1Amount").text = str(int(sheet.Range("D135").Value))
        if sheet.Range("E135").Value != "NA" and sheet.Range("E135").Value != 0:
            ET.SubElement(info, "FinancialCoCommercialPaperMonth2Amount").text = str(int(sheet.Range("E135").Value))
        if sheet.Range("F135").Value != "NA" and sheet.Range("F135").Value != 0:
            ET.SubElement(info, "FinancialCoCommercialPaperMonth3Amount").text = str(int(sheet.Range("F135").Value))

        # 63q
        if sheet.Range("D136").Value != "NA" and sheet.Range("D136").Value != 0:
            ET.SubElement(info, "NonFinancialCoCommercialPaperMonth1Amount").text = str(int(sheet.Range("D136").Value))
        if sheet.Range("E136").Value != "NA" and sheet.Range("E136").Value != 0:
            ET.SubElement(info, "NonFinancialCoCommercialPaperMonth2Amount").text = str(int(sheet.Range("E136").Value))
        if sheet.Range("F136").Value != "NA" and sheet.Range("F136").Value != 0:
            ET.SubElement(info, "NonFinancialCoCommercialPaperMonth3Amount").text = str(int(sheet.Range("F136").Value))

        # 63r
        if sheet.Range("D137").Value != "NA" and sheet.Range("D137").Value != 0:
            ET.SubElement(info, "TenderOptionBondMonth1Amount").text = str(int(sheet.Range("D137").Value))
        if sheet.Range("E137").Value != "NA" and sheet.Range("E137").Value != 0:
            ET.SubElement(info, "TenderOptionBondMonth2Amount").text = str(int(sheet.Range("E137").Value))
        if sheet.Range("F137").Value != "NA" and sheet.Range("F137").Value != 0:
            ET.SubElement(info, "TenderOptionBondMonth3Amount").text = str(int(sheet.Range("F137").Value))

        # 63rs
        if sheet.Range("D138").Value != "NA" and sheet.Range("D138").Value != 0:
            ET.SubElement(info, "OtherInstrumentMonth1Amount").text = str(int(sheet.Range("D138").Value))
        if sheet.Range("E138").Value != "NA" and sheet.Range("E138").Value != 0:
            ET.SubElement(info, "OtherInstrumentMonth2Amount").text = str(int(sheet.Range("E138").Value))
        if sheet.Range("F138").Value != "NA" and sheet.Range("F138").Value != 0:
            ET.SubElement(info, "OtherInstrumentMonth3Amount").text = str(int(sheet.Range("F138").Value))

        # TODO: Question 63s: Instrument description, what does it look like?


# TODO: verify with Heather on this new section
def section3_itemG(wb):
    infolist = ET.SubElement(
        root, "PFSection3ItemGLiquidityParallelMoneyMarketInfoList"
    )

    for sheet_name in SECTION_3_SHEETS2:
        sheet = wb.Worksheets(sheet_name)
        info = ET.SubElement(
            infolist, "PFSection3ItemGLiquidityParallelMoneyMarketInfo"
        )
        ET.SubElement(info, "FundID").text = sheet.Range("A1").Value
        ET.SubElement(info, "MMFSeriesNumber").text = sheet.Range("D141").Value


def open_workbook(xlapp, xlfile):
    """
    Helper function to open the workbook.
    Requires an excel instance and a workbook path
    """
    try:
        xlwb = xlapp.Workbooks(xlfile)
    except Exception as e:
        try:
            xlwb = xlapp.Workbooks.Open(
                xlfile, 0, 1
            )  # don't update links, open read only
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
