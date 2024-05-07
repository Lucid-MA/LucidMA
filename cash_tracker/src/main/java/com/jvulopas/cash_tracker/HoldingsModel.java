package com.jvulopas.cash_tracker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class HoldingsModel {
	
	private Set<Fund> funds;
	private Date valDate;
	private Workbook initWB;
	
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMdd");
	public static final double pairoffDiffThreshold = 5; //$ 5.00
	
	private static HashMap<String, String> colToAcct = new HashMap<String, String>(); // for CFs page
	
	/**
	 * Initialize a new holdings model from worksheet.
	 * Strict invariants enforced wrt workbook structure and format.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws NullPointerException
	 */
	public HoldingsModel(String initializerFilePath, Date valDate) throws FileNotFoundException, IOException, NullPointerException {
		this.valDate = valDate;
		FileInputStream fis = new FileInputStream(new File(initializerFilePath));
		initWB = new XSSFWorkbook(fis);
		Sheet sht = initWB.getSheet("Main");
		int dateRow = 1;
		Cell curr, seriesCell;
		
		colToAcct.put("C", "MAIN");
		colToAcct.put("E", "EXPENSE");
		colToAcct.put("G", "MARGIN");
		colToAcct.put("I", "MANAGEMENT");
		colToAcct.put("K", "SUBSCRIPTION");
		
		// initialize fund data
		funds = new HashSet<Fund>();
		int tableMapRowStart = 0, balancesRowStart = 0;
		for (int j = 1; j < 1000; j++) {
			if (sht.getRow(j) == null) {
				continue;
			}
			curr = sht.getRow(j).getCell(CellReference.convertColStringToIndex("B"));
			if (curr == null) {
				continue;
			}
			
			String val = "";
			try {
				val = curr.getStringCellValue();
			} catch (Exception exc) {
				continue;
			}
			
			if (val.equals("Account Table")) {
				tableMapRowStart = j + 2;
			} else if (val.equals("Date")) {
				dateRow = curr.getRowIndex();
				if (sht.getRow(j + 1).getCell(CellReference.convertColStringToIndex("C")).getDateCellValue().equals(valDate)) {
					throw new IOException("TrackerState file wrong-- CoB not valid.");
				}
			} else if (val.equals("Balances Table")) {
				balancesRowStart = j + 2;
				break;
			}
		}
		
		
		// BNYM accounts
		curr = sht.getRow(tableMapRowStart).getCell(CellReference.convertColStringToIndex("B"));
		while (curr.getRowIndex() < dateRow) {
			String fundName = curr.getStringCellValue();
			HashMap<String, Integer> accts = new HashMap<String, Integer>();
			for (int i = 2; i < 7; i++) {
				accts.put(sht.getRow(tableMapRowStart - 1).getCell(i).getStringCellValue(), 
						(int) sht.getRow(curr.getRowIndex()).getCell(i).getNumericCellValue());
			}
			
			String sweepVehicle = sht.getRow(curr.getRowIndex()).getCell(7).getStringCellValue();
			
			
			
			seriesCell = sht.getRow(4).getCell(CellReference.convertColStringToIndex("S"));
			HashMap<String, Double> seriesToAdd = new HashMap<String, Double>();
			while (!cellEmpty(seriesCell)) {
				if (seriesCell.getStringCellValue().equals(fundName)) {
					seriesToAdd.put(seriesCell.getRow().getCell(CellReference.convertColStringToIndex("T")).getStringCellValue(), 
							seriesCell.getRow().getCell(CellReference.convertColStringToIndex("U")).getNumericCellValue());
				}
				seriesCell = sht.getRow(seriesCell.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("S"));
			}
			
			funds.add(new Fund(fundName, accts, seriesToAdd, sweepVehicle));
			
			if (sht.getRow(curr.getRowIndex()+1) == null) {
				break;
			}
			curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
		}
		
		// cash and sweep balances
		curr = sht.getRow(balancesRowStart).getCell(CellReference.convertColStringToIndex("B"));
		while (!cellEmpty(curr)) {
			
			String fundName = curr.getStringCellValue();
			
			Fund f = getFundByName(fundName);
			// just cash here, do mm sweep vehicle in 'other security holdings'
			f.uploadInitialPosition(
					curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue(), 
					HoldingsModel.getCashRepresentation(), 
					curr.getRow().getCell(CellReference.convertColStringToIndex("D")).getNumericCellValue(), 
					HoldingStatus.AVAILABLE
			);
			// redundant; doing it here double counts.
//			f.uploadInitialPosition(
//					curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue(), 
//					f.getSweepVehicle(), 
//					curr.getRow().getCell(CellReference.convertColStringToIndex("E")).getNumericCellValue(), 
//					HoldingStatus.AVAILABLE
//			);
			
			if (sht.getRow(curr.getRowIndex()+1) == null) {
				break;
			}
			curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
		}
		
		// other security holdings
		curr = sht.getRow(4).getCell(CellReference.convertColStringToIndex("L"));
		while (!cellEmpty(curr)) {
			String fundName = curr.getStringCellValue();
			Fund f = getFundByName(fundName);
			f.uploadInitialPosition(
					curr.getRow().getCell(CellReference.convertColStringToIndex("M")).getStringCellValue(), 
					curr.getRow().getCell(CellReference.convertColStringToIndex("N")).getStringCellValue(), 
					curr.getRow().getCell(CellReference.convertColStringToIndex("P")).getNumericCellValue(), 
					statusStringToEnum(curr.getRow().getCell(CellReference.convertColStringToIndex("O")).getStringCellValue())
			);
			if (sht.getRow(curr.getRowIndex()+1) == null) {
				break;
			}
			curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("L"));
		}
		
		// failing trades 
		
		int failsRowStart = 0;
		
		for (int j = 1; j < 1000; j++) {
			if (sht.getRow(j) == null) {
				continue;
			}
			curr = sht.getRow(j).getCell(CellReference.convertColStringToIndex("B"));
			if (curr == null) {
				continue;
			}
			
			String val = "";
			try {
				val = curr.getStringCellValue();
			} catch (Exception exc) {
				continue;
			}
			
			if (val.equals("Failing Trades")) {
				failsRowStart = j + 2;
				break;
			}
		}
		
		if (sht.getRow(failsRowStart) == null) {
			sht.createRow(failsRowStart);
		}
		curr = sht.getRow(failsRowStart).getCell(CellReference.convertColStringToIndex("B"));
		
		Date tDate;
		Fund tFund;
		String tDesc = "";
		String tActionID = "";
		String tAcct = "";
		double tAmount = 0;
		
		boolean foundTransaction = false;
		
		HashMap<Fund, HashSet<Transaction>> previousFailsToUpload = new HashMap<Fund, HashSet<Transaction>>();
		
		while (!cellEmpty(curr)) {
			try {
				Transaction tran;
				tDate = curr.getDateCellValue();
				tFund = this.getFundByName(curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue());
				
				if (!previousFailsToUpload.containsKey(tFund)) {
					previousFailsToUpload.put(tFund, new HashSet<Transaction>());
				}
				
				tAcct = curr.getRow().getCell(CellReference.convertColStringToIndex("D")).getStringCellValue();
				
				tAmount = curr.getRow().getCell(CellReference.convertColStringToIndex("E")).getNumericCellValue();
				tActionID = curr.getRow().getCell(CellReference.convertColStringToIndex("F")).getStringCellValue();
				tDesc = curr.getRow().getCell(CellReference.convertColStringToIndex("G")).getStringCellValue();
				
				foundTransaction = false;
				
				for (Transaction t: previousFailsToUpload.get(tFund)) {
					if (t.getActionID().equals(tActionID) && t.getDescription().equals(tDesc) && t.getDate().isPresent()) {
						if (t.getDate().get().equals(tDate)) { // if same date
							foundTransaction = true;
							Flow toAdd = new Flow(tFund.getAcctByName(tAcct), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, tAmount, t); 
							t.addFlow(toAdd);
							//tFund.getAcctByName(tAcct).uploadFlow(toAdd);
							// when appending a flow to a pre existing transaction, must manually upload the flow to the account.
							// otherwise won't register at master level
							// TODO fix that, messy encapsulation there
						}
					}
				}
				
				if (!foundTransaction) {
					tran = new Transaction(tActionID, tDesc, tDate);
					tran.addFlow(new Flow(tFund.getAcctByName(tAcct), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, tAmount, tran));
					//tFund.uploadPreviousFail(tran);
					previousFailsToUpload.get(tFund).add(tran);
				}
				
				if (sht.getRow(curr.getRowIndex()+1) == null) {
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
			} catch (Exception e) {
				System.out.println("Error recording failing trades");
				e.printStackTrace();
			}
		}
		
		for (Fund tF: previousFailsToUpload.keySet()) {
			for (Transaction failingT : previousFailsToUpload.get(tF)) {
				tF.uploadPreviousFail(failingT);
			}
		}
		
		// upload initial series holdings
		int shtI = 1;
		sht = initWB.getSheetAt(shtI);
		
		// assumes all series in initial workbook
		
		while (sht != null) {
			Fund f = getFundByName(sht.getRow(2).getCell(CellReference.convertColStringToIndex("C")).getStringCellValue());
			String seriesName = sht.getRow(3).getCell(CellReference.convertColStringToIndex("C")).getStringCellValue();
			curr = sht.getRow(8).getCell(CellReference.convertColStringToIndex("B"));
			
			// allocate cash
			while (!cellEmpty(curr)) {
				f.allocateInitialPosition(
						seriesName, 
						curr.getStringCellValue(), 
						HoldingsModel.getCashRepresentation(), 
						curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getNumericCellValue(), 
						HoldingStatus.AVAILABLE
				);
				
				if (sht.getRow(curr.getRowIndex()+1) == null) {
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
			}
			
			// allocate other sec holdings
			curr = sht.getRow(5).getCell(CellReference.convertColStringToIndex("H"));
			while (!cellEmpty(curr)) {
				f.allocateInitialPosition(
						seriesName, 
						curr.getStringCellValue(), 
						curr.getRow().getCell(CellReference.convertColStringToIndex("I")).getStringCellValue(), 
						curr.getRow().getCell(CellReference.convertColStringToIndex("K")).getNumericCellValue(), 
						statusStringToEnum(curr.getRow().getCell(CellReference.convertColStringToIndex("J")).getStringCellValue())
				);
				
				if (sht.getRow(curr.getRowIndex()+1) == null) {
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("H"));
			}
			
			// allocate previous fails
			curr = sht.getRow(5).getCell(CellReference.convertColStringToIndex("N"));
			Date fDt = null;
			String fAcct = "";
			String fActionID = "";
			double fAmount = 0;
			double fMasterAmount = 0;
			List<Transaction> fPreviousFails = f.getPreviousFails();
			Set<String> hereAlreadyAllocated = new HashSet<String>(); // remember that we assume that all flows in a transaction are allocated equally. so we only read in and allocated a previous fail transaction ONCE
			// that is, flows are recorded and saved down (for calcing projecteds), but *transactions* are used to allocated. so allocated two flows from same transaction separately will double count
			while (!cellEmpty(curr)) {
				fMasterAmount = 0;
				fDt = curr.getDateCellValue();
				fAcct = curr.getRow().getCell(CellReference.convertColStringToIndex("O")).getStringCellValue();
				fAmount = curr.getRow().getCell(CellReference.convertColStringToIndex("P")).getNumericCellValue();
				fActionID = curr.getRow().getCell(CellReference.convertColStringToIndex("Q")).getStringCellValue();
				if (!hereAlreadyAllocated.contains(fActionID)) {
					hereAlreadyAllocated.add(fActionID);
					for (Transaction fT : fPreviousFails) {
						if (fT.getActionID().equals(fActionID)) {
							if (fT.getDate().isPresent()) {
								if (HoldingsModel.sameDate(fDt, fT.getDate().get())) {
									// then allocation of that same thing
									fMasterAmount = fT.getFlowAmountToAccount(f.getAcctByName(fAcct), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE); // for now just available cash
								}
							}
						}
					}
					if (fMasterAmount != 0) {
						f.allocate(
								seriesName, 
								fActionID,
								(100.0 * fAmount / fMasterAmount)/100.0
						);
					}
				}
				if (sht.getRow(curr.getRowIndex()+1) == null) {
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("N"));
			}
			
			
			
			
			
			shtI++;
			try {
				sht = initWB.getSheetAt(shtI);
			} catch (Exception e) {
				break; // all done
			}
		}
		
		// close initializer
		fis.close();
		initWB.close();

		
	}
	
	/**
	 * Save cash securities holdings to excel file.
	 * Assumes standard format from template...
	 * Assumes no funds/series added intraday; format must be updated manually for now.
	 * Assumes the 5 BNYM account names standard across funds. 
	 * 
	 * @param templatePath
	 * @param savePath
	 * @param backupPath
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws NullPointerException
	 */
	public void saveStateToXLSX(String templatePath, String savePath, String backupPath) throws FileNotFoundException, IOException, NullPointerException {
		
		FileInputStream fis = new FileInputStream(new File(templatePath));
		Workbook templateWB = new XSSFWorkbook(fis);
		Sheet sht = templateWB.getSheet("Main");
		
		
		
		// change: made this stuff pre-loaded. fine invariant to assume it won't change intraday/
//		for (Fund f: funds) {
//			sht.getRow(crow).getCell(ccol).setCellValue(f.getFundID()); // fundname
//			ccol++;
//
//			for (ccol = CellReference.convertColStringToIndex("C"); ccol < CellReference.convertColStringToIndex("H"); ccol++) {
//				sht.getRow(crow).getCell(ccol).setCellValue(f.getAcctByName(sht.getRow(3).getCell(ccol).getStringCellValue()).getAcctNumber()); // set account numbers
//			}
//			ccol = CellReference.convertColStringToIndex("B"); // reset col
//			crow++; // reset row
//		}
		
		// get starts so don't have to change if num funds changes
		Cell curr;
		Row currentRow;
		
		int balancesRowStart = 0;
		for (int j = 1; j < 1000; j++) {
			if (sht.getRow(j) == null) {
				continue;
			}
			curr = sht.getRow(j).getCell(CellReference.convertColStringToIndex("B"));
			if (curr == null) {
				continue;
			}
			
			String val; 
			
			try {
				val = curr.getStringCellValue();
			} catch (Exception e) {
				continue;
			}
			
			if (val.equals("Date")) {
				sht.getRow(j + 1).getCell(CellReference.convertColStringToIndex("C")).setCellValue(this.valDate);
				System.out.println(valDate);
			} else if (val.equals("Balances Table")) {
				balancesRowStart = j + 2;
				break;
			}
		}
		
		
		
		// write EOD cash and sweep balances
		curr = sht.getRow(balancesRowStart).getCell(CellReference.convertColStringToIndex("B"));
		while (!cellEmpty(curr)) {
			Fund f = getFundByName(curr.getStringCellValue());
			String acctName = curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue();
			curr.getRow().getCell(CellReference.convertColStringToIndex("D")).setCellValue(f.getAcctByName(acctName).getEODPosition(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE));
			curr.getRow().getCell(CellReference.convertColStringToIndex("E")).setCellValue(f.getAcctByName(acctName).getEODPosition(f.getSweepVehicle(), HoldingStatus.AVAILABLE));
			curr.getRow().getCell(CellReference.convertColStringToIndex("F")).setCellValue(f.getAcctByName(acctName).getProjectedEODPosition(f.getSweepVehicle(), HoldingStatus.AVAILABLE) + f.getAcctByName(acctName).getProjectedEODPosition(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE));
			// move cursor
			if (sht.getRow(curr.getRowIndex()+1) == null) {
				break;
			}
			curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
		}
		
		// other security holdings
		curr = sht.getRow(4).getCell(CellReference.convertColStringToIndex("L"));
		for (Fund f: this.funds) {
			for (String acctName: f.getAccountNames()) {
				HashMap<String, Double> availableHoldings = f.getAcctByName(acctName).getEODHoldingsSet(HoldingStatus.AVAILABLE);
				HashMap<String, Double> collateralHoldings = f.getAcctByName(acctName).getEODHoldingsSet(HoldingStatus.REPO_COLLATERAL);
				for (String sec : availableHoldings.keySet()) {
					if (sec.equals(HoldingsModel.getCashRepresentation())) {
						continue;
					}
					
					// TODO for now, only write the sweep balances
					if (!sec.equals(f.getSweepVehicle())) {
						continue;
					}					
					
					curr.setCellValue(f.getFundID()); // fund name
					curr.getRow().getCell(CellReference.convertColStringToIndex("M"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(acctName); // acct name
					curr.getRow().getCell(CellReference.convertColStringToIndex("N"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(sec); // security
					curr.getRow().getCell(CellReference.convertColStringToIndex("O"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("AVAILABLE"); // status
					curr.getRow().getCell(CellReference.convertColStringToIndex("P"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(availableHoldings.get(sec)); // amount
					// move cursor
					currentRow = sht.getRow(curr.getRowIndex() + 1);
					if (currentRow == null) {
						currentRow = sht.createRow(curr.getRowIndex() + 1);
					}
					curr = currentRow.getCell(CellReference.convertColStringToIndex("L"));
					if (curr == null) {
						curr = currentRow.createCell(CellReference.convertColStringToIndex("L"));
					}
				}
				
				for (String sec : collateralHoldings.keySet()) {
					if (sec.equals(HoldingsModel.getCashRepresentation())) {
						continue;
					}
					// TODO for now, only write the sweep balances
					if (!sec.equals(f.getSweepVehicle())) {
						continue;
					}		
					curr.setCellValue(f.getFundID()); // fund name
					curr.getRow().getCell(CellReference.convertColStringToIndex("M"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(acctName); // acct name
					curr.getRow().getCell(CellReference.convertColStringToIndex("N"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(sec); // security
					curr.getRow().getCell(CellReference.convertColStringToIndex("O"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("REPO COLLATERAL"); // status
					curr.getRow().getCell(CellReference.convertColStringToIndex("P"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(collateralHoldings.get(sec)); // amount
					// move cursor
					currentRow = sht.getRow(curr.getRowIndex() + 1);
					if (currentRow == null) {
						currentRow = sht.createRow(curr.getRowIndex() + 1);
					}
					curr = currentRow.getCell(CellReference.convertColStringToIndex("L"),Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
				}
			}
		}
		
		
		// TODO this is just fails on available cash for now
		int failsRowStart = 0;
		
		for (int j = 1; j < 1000; j++) {
			if (sht.getRow(j) == null) {
				continue;
			}
			curr = sht.getRow(j).getCell(CellReference.convertColStringToIndex("B"));
			if (curr == null) {
				continue;
			}
			
			String val = "";
			try {
				val = curr.getStringCellValue();
			} catch (Exception exc) {
				continue;
			}
			
			if (val.equals("Failing Trades")) {
				failsRowStart = j + 2;
				break;
			}
		}
		
		if (sht.getRow(failsRowStart) == null) {
			sht.createRow(failsRowStart);
		}
		curr = sht.getRow(failsRowStart).getCell(CellReference.convertColStringToIndex("B"));
		
		Date tDate;
		
		for (Fund f: funds) {
			for (Transaction t : f.getTransactions()) {
				if (t.getDate().isPresent()) {
					tDate = t.getDate().get();
				} else {
					tDate = valDate;
				}
				
				for (Flow fl: t.getFlows()) {
					if (fl.getSecurity().equals(HoldingsModel.getCashRepresentation()) && fl.getStatus().equals(HoldingStatus.AVAILABLE)) {
						if (fl.hasSettled().isPresent()) {
							if (!fl.hasSettled().get()) {
								// if failing
								try {
									curr.getRow().getCell(CellReference.convertColStringToIndex("B"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(SDF.parse(SDF.format(tDate)));							
									curr.getRow().getCell(CellReference.convertColStringToIndex("C"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(this.getFundByBNYMAccountID(fl.getAccount().getAcctNumber()).getFundID());
									curr.getRow().getCell(CellReference.convertColStringToIndex("D"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fl.getAccount().getName());
									curr.getRow().getCell(CellReference.convertColStringToIndex("E"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fl.getAmount());
									curr.getRow().getCell(CellReference.convertColStringToIndex("F"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(t.getActionID());
									curr.getRow().getCell(CellReference.convertColStringToIndex("G"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(t.getDescription());
								} catch (Exception e) {
									System.out.println("Error encountered recording failing trades");
								}
								
								// increment row
								if (sht.getRow(curr.getRowIndex()+1) == null) {
									break;
								}
								curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
							}
						}
					}
				}
				
			}
		}
		
		
		// write encumbered cash balances (cash margin posted to counterparties)
		int cRow = 4;
		curr = sht.getRow(cRow).getCell(CellReference.convertColStringToIndex("X"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
		for (Fund fnd : funds) {
			HashMap<String, Double> cpCash = fnd.getCounterpartyCash();
			for (String cp : cpCash.keySet()) {
				if (cpCash.get(cp) < 0) {
					curr.setCellValue(fnd.getFundID());
					curr.getRow().getCell(CellReference.convertColStringToIndex("Y"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(cp);
					curr.getRow().getCell(CellReference.convertColStringToIndex("Z"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(cpCash.get(cp));
					cRow++;
					if (sht.getRow(cRow) == null) {
						curr = sht.createRow(cRow).getCell(CellReference.convertColStringToIndex("X"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					} else {
						curr = sht.getRow(cRow).getCell(CellReference.convertColStringToIndex("X"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					}
				}
			}
		}

		// series pages (see invariants in javadoc comment)
		int shtI = 1;
		sht = templateWB.getSheetAt(shtI);
		while (sht != null) {
			Fund f = getFundByName(sht.getRow(2).getCell(CellReference.convertColStringToIndex("C")).getStringCellValue());
			String seriesName = sht.getRow(3).getCell(CellReference.convertColStringToIndex("C")).getStringCellValue();
			
			// cash and sweep balances
			curr = sht.getRow(8).getCell(CellReference.convertColStringToIndex("B"));
			while (!cellEmpty(curr)) {
				String acctName = curr.getRow().getCell(CellReference.convertColStringToIndex("B")).getStringCellValue();
				curr.getRow().getCell(CellReference.convertColStringToIndex("C")).setCellValue(f.getEODPositionInSeriesInAcct(seriesName, acctName, HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE));
				curr.getRow().getCell(CellReference.convertColStringToIndex("D")).setCellValue(f.getEODPositionInSeriesInAcct(seriesName, acctName, f.getSweepVehicle(), HoldingStatus.AVAILABLE));
				curr.getRow().getCell(CellReference.convertColStringToIndex("E")).setCellValue(f.getEODPositionInSeriesInAcct(seriesName, acctName, f.getSweepVehicle(), HoldingStatus.AVAILABLE) + f.getProjectedEODPositionInSeriesInAcct(seriesName, acctName, HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE));
				// move cursor
				if (sht.getRow(curr.getRowIndex()+1) == null) {
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"));
			}
			
			// other security holdings
			curr = sht.getRow(5).getCell(CellReference.convertColStringToIndex("H"));
			for (String acctName: f.getAccountNames()) {
				HashMap<String, Double> availableHoldings = f.getEODHoldingsSetInSeriesInAcct(seriesName, acctName, HoldingStatus.AVAILABLE);
				HashMap<String, Double> collateralHoldings = f.getEODHoldingsSetInSeriesInAcct(seriesName, acctName, HoldingStatus.REPO_COLLATERAL);
				for (String sec : availableHoldings.keySet()) {
					if (sec.equals(HoldingsModel.getCashRepresentation())) {
						continue;
					}
					// TODO for now, only write the sweep balances
					if (!sec.equals(f.getSweepVehicle())) {
						continue;
					}
					curr.setCellValue(acctName); // acct name
					curr.getRow().getCell(CellReference.convertColStringToIndex("I"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(sec); // security
					curr.getRow().getCell(CellReference.convertColStringToIndex("J"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("AVAILABLE"); // status
					curr.getRow().getCell(CellReference.convertColStringToIndex("K"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(availableHoldings.get(sec)); // amount
					// move cursor
					currentRow = sht.getRow(curr.getRowIndex() + 1);
					if (currentRow == null) {
						currentRow = sht.createRow(curr.getRowIndex() + 1);
					}
					curr = currentRow.getCell(CellReference.convertColStringToIndex("H"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
				}
				
				for (String sec : collateralHoldings.keySet()) {
					if (sec.equals(HoldingsModel.getCashRepresentation())) {
						continue;
					}
					// TODO for now, only write the sweep balances
					if (!sec.equals(f.getSweepVehicle())) {
						continue;
					}
					curr.setCellValue(acctName); // acct name
					curr.getRow().getCell(CellReference.convertColStringToIndex("I"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(sec); // security
					curr.getRow().getCell(CellReference.convertColStringToIndex("J"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("REPO COLLATERAL"); // status
					curr.getRow().getCell(CellReference.convertColStringToIndex("K"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(collateralHoldings.get(sec)); // amount
					// move cursor
					currentRow = sht.getRow(curr.getRowIndex() + 1);
					if (currentRow == null) {
						currentRow = sht.createRow(curr.getRowIndex() + 1);
					}
					curr = currentRow.getCell(CellReference.convertColStringToIndex("H"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
				}
			}
			
			// encumbered cash margin posted to counterparties
			cRow = 17;
			curr = sht.getRow(cRow).getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
			HashMap<String, Double> cpCash = f.getCounterpartyCashInSeries(seriesName);
			for (String cp : cpCash.keySet()) {
				if (cpCash.get(cp) < 0) {
					curr.setCellValue(cp);
					curr.getRow().getCell(CellReference.convertColStringToIndex("C"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(cpCash.get(cp));
					cRow++;
					if (sht.getRow(cRow) == null) {
						curr = sht.createRow(cRow).getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					} else {
						curr = sht.getRow(cRow).getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					}
				}
			}
			

			// fails
			// TODO this is just fails of available cash for now, eventually just make property of any holding status
			curr = sht.getRow(5).getCell(CellReference.convertColStringToIndex("N"));
			
			Date fDate = null;
			
			for (Transaction t : f.getTransactionsInSeries(seriesName, HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE)) {
				if (t.getDate().isPresent()) {
					fDate = t.getDate().get();
				} else {
					fDate = valDate;
				}
				
				for (Flow fl: t.getFlows()) {
					if (fl.getSecurity().equals(HoldingsModel.getCashRepresentation()) && fl.getStatus().equals(HoldingStatus.AVAILABLE)) {
						if (fl.hasSettled().isPresent()) {
							if (!fl.hasSettled().get()) {
								// if failing
								try {
									curr.getRow().getCell(CellReference.convertColStringToIndex("N"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(SDF.parse(SDF.format(fDate)));							
									curr.getRow().getCell(CellReference.convertColStringToIndex("O"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fl.getAccount().getName());
									curr.getRow().getCell(CellReference.convertColStringToIndex("P"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fl.getAmount());
									curr.getRow().getCell(CellReference.convertColStringToIndex("Q"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(t.getActionID());
								} catch (Exception e) {
									System.out.println("Error recording failing trades");
								}
								
								// increment row
								if (sht.getRow(curr.getRowIndex()+1) == null) {
									break;
								}
								curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("N"));
							}
						}
					}
				}
				
			}
			
			
			shtI++;
			try {
				sht = templateWB.getSheetAt(shtI);
			} catch (Exception e) {
				break;
			}
		}
		
		FileOutputStream save = new FileOutputStream(new File(savePath));
		templateWB.write(save);
		save.close();
		if (backupPath != null) {
			save = new FileOutputStream(new File(backupPath));
			templateWB.write(save);
			save.close();
		}
		templateWB.close();
		fis.close();		
	}
	
	
	public static boolean sameDate(Date date1, Date date2) {
		if (date1 == null && date2 == null) return true;
		if (date1 == null || date2 == null) {
			return false;
		}
		return SDF.format(date1).equals(SDF.format(date2));
	}
	
	public static HoldingStatus statusStringToEnum(String s) throws IOException {
		if (s.toLowerCase().trim().equals("available")) {
			return HoldingStatus.AVAILABLE;
		} else if (s.toLowerCase().trim().equals("repo collateral")) {
			return HoldingStatus.REPO_COLLATERAL;
		} else {
			throw new IOException("Illegal format: holding status " + s);
		}
		
	}
	
	public Fund getFundByName(String name) {
		for (Fund f: funds) {
			if (f.getFundID().equals(name)) {
				return f;
			}
		}
		return null;
	}
	
	public Set<String> getFundNames() {
		Set<String> outp = new HashSet<String>();
		for (Fund f : funds) {
			outp.add(f.getFundID());
		}
		return outp;
	}
	
	public static boolean cellEmpty(Cell cell) {
		if (cell == null) return true;
//		if (cell.getCellType() == Cell.CELL_TYPE_BLANK) return true;
//		if (cell.getCellType() == Cell.CELL_TYPE_STRING && cell.getStringCellValue().trim().isBlank()) return true;

		try {
			return cell.getStringCellValue().trim().isBlank();
		} catch (Exception e) {
			return false;
		}
	}

	/*
	 * For modularity
	 */
	public static String getCashRepresentation() {
		return "CASHUSD01";
	}

	public boolean allAllocated() {
		return notAllocated().isEmpty();
	}
	
	public HashSet<Transaction> notAllocated() {
		HashSet<Transaction> outp = new HashSet<Transaction>();
		
		for (Fund f: funds) {
			for (Transaction t: f.notAllocated()) {
				outp.add(t);
			}
		}
		return outp;
	}
	
	/**
	 * A catcher for some edge cases to handle text input
	 * @param s
	 * @param f
	 * @return
	 */
	public static String securitizeString(String s, Fund f) {
		s=s.toLowerCase().strip();
		if (s.equals("cash") || s.equals("usd cash") || s.equals("usd")) {
			return getCashRepresentation();
		}
		
		if (s.equals("sweep") || s.equals("mm sweep") || s.equals("dgcxx equity") || s.equals("dgcxx") || s.equals("sweep vehicle")) {
			return f.getSweepVehicle();
		}
		return s.toUpperCase().strip();
	}
	
	public String getAcctNameFromBNYMID(int acctID) throws NoSuchElementException {
		for (Fund f: funds) {
			for (String name: f.getAccountNames()) {
				if (f.getAcctByName(name).getAcctNumber() == acctID) {
					return name;
				}
			}
		}
		throw new NoSuchElementException("Account " + acctID + " does not exist.");
	}

	public Fund getFundByBNYMAccountID(int fromAcctID) throws NoSuchElementException {
		for (Fund f: funds) {
			for (String name: f.getAccountNames()) {
				if (f.getAcctByName(name).getAcctNumber() == fromAcctID) {
					return f;
				}
			}
		}
		throw new NoSuchElementException("Account " + fromAcctID + " does not exist.");
	}
	
	public void transactPairoffs() {
		for (Fund f : funds) {
			f.uploadPairoffsIntoTransactions();
		}		
	}

	/*
	 * MM Sweep across all funds
	 */
	public void mmSweep() {
		for (Fund f: funds) {
			f.MMSweep();
		}
	}

	@SuppressWarnings("resource")
	public void bnymCashReconc(String bnyFilepath) throws IOException {
		
		// perform cash reconciliation against BNYM
		
		FileInputStream fistemp = new FileInputStream(new File("Expected Cash Flows Template.xlsx"));
		Workbook expectedCFTemplate = new XSSFWorkbook(fistemp);
		Sheet reconcSheet = expectedCFTemplate.getSheet("Reconciliation");
		HashMap<Integer, Flow> rowToFlow = new HashMap<Integer, Flow>();
		
		Cell curr;
		HashSet<Fund> sweepDetected = new HashSet<Fund>();
		// gather BNY reconc file data (do first in case MM Sweep detected and must make manual adjustments)
		
		
		Sheet bnyTosht = expectedCFTemplate.getSheet("BNYM Report");
		try {
			FileInputStream fis = new FileInputStream(new File(bnyFilepath));
			Workbook bnyFile = new HSSFWorkbook(fis);
			Sheet bnyFromsht = bnyFile.getSheetAt(0);
			try {
				curr = bnyFromsht.getRow(1).getCell(CellReference.convertColStringToIndex("A"));
			} catch (NullPointerException e) {
				curr = null;
			}
			Cell fromCell;
			while (!cellEmpty(curr)) {
				if (bnyTosht.getRow(curr.getRowIndex() + 3) == null) {
					bnyTosht.createRow(curr.getRowIndex() + 3);
				}
				for (int i = 0; i < 18; i++) {
					fromCell = bnyFromsht.getRow(curr.getRowIndex()).getCell(i);
					if (cellEmpty(fromCell)) {
						bnyTosht.getRow(curr.getRowIndex() + 3).getCell(i + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("");
					} else {
						// current offset is (3,2)
						if (i == 0) {
							try {
								bnyTosht.getRow(curr.getRowIndex() + 3).getCell(i + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fromCell.getStringCellValue());
							} catch (Exception e) {
									bnyTosht.getRow(curr.getRowIndex() + 3).getCell(i + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fromCell.getNumericCellValue()+ "");
							}
						} else {
							
							// check if sweep has occurred
							if (i == 16) {
								try {
									if (fromCell.getStringCellValue().equals("STIF LOCATIONS") && !bnyFromsht.getRow(curr.getRowIndex()).getCell(5).getStringCellValue().equals("DIVIDEND")) {
										sweepDetected.add(getFundByBNYMAccountID(Integer.parseInt(bnyFromsht.getRow(curr.getRowIndex()).getCell(0).getStringCellValue().substring(0, 6))));
									}
								} catch (IllegalStateException exc) {
									// nothing to do... continue 
								}
							}
							
							try {
								try {
									bnyTosht.getRow(curr.getRowIndex() + 3).getCell(i + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fromCell.getStringCellValue());
								} catch (IllegalStateException exc) {
									bnyTosht.getRow(curr.getRowIndex() + 3).getCell(i + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fromCell.getNumericCellValue());
								} 
							} catch (IllegalStateException exc) {
								bnyTosht.getRow(curr.getRowIndex() + 3).getCell(curr.getColumnIndex() + i + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(fromCell.getDateCellValue());
							}
						}
					}
				}
				if (bnyFromsht.getRow(curr.getRowIndex() + 1) != null) {
					curr = bnyFromsht.getRow(curr.getRowIndex() + 1).getCell(CellReference.convertColStringToIndex("A"));
				} else {
					break;
				}
			}
			
			bnyFile.close();
			fis.close();
		} catch (FileNotFoundException fnfe) {
			
		}
		
		// generate expected cash flow tab
		// set to nowTODO if historical set to valdate EOD
		Cell timecell = reconcSheet.getRow(1).getCell(CellReference.convertColStringToIndex("C"));
		timecell.setCellValue(new SimpleDateFormat("MM/dd/YYYY HH:mm:ss").format(new Date()));
		
		curr = reconcSheet.getRow(5).getCell(CellReference.convertColStringToIndex("B"), MissingCellPolicy.CREATE_NULL_AS_BLANK);
		for (Fund f : funds) {
			for (Transaction t : f.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE)) {
				// TODO for now just cash, can eventually be any security, any status
				if (t.getActionID().length()>=8) {
					if (t.getActionID().substring(0, 8).toUpperCase().equals("REALLOC_")) {
						continue; // ignore inner reallocations
					}
				}
				for (Flow flow : t.getFlows()) {
					// if flow of available cash
					if (flow.getSecurity().equals(HoldingsModel.getCashRepresentation()) && flow.getStatus().equals(HoldingStatus.AVAILABLE)) {
						rowToFlow.put(curr.getRowIndex(), flow);
						String colLetter = "C"; // into acct
						if (flow.getAmount() <= 0) {
							colLetter = "B"; // from acct
						}
						reconcSheet.getRow(curr.getRowIndex()).getCell(CellReference.convertColStringToIndex(colLetter)).setCellValue("" + flow.getAccount().getAcctNumber());
						reconcSheet.getRow(curr.getRowIndex()).getCell(CellReference.convertColStringToIndex("D")).setCellValue(Math.abs(flow.getAmount()));
						if (t.getHelixID().isPresent()) {
							reconcSheet.getRow(curr.getRowIndex()).getCell(CellReference.convertColStringToIndex("E"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(t.getHelixID().get());
						} else {
							reconcSheet.getRow(curr.getRowIndex()).getCell(CellReference.convertColStringToIndex("E"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("");
						}
						reconcSheet.getRow(curr.getRowIndex()).getCell(CellReference.convertColStringToIndex("F"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(t.getDescription());
						curr = reconcSheet.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("B"), MissingCellPolicy.CREATE_NULL_AS_BLANK);
					}
				}
				
			}
		}
		

		
		// compare
		Cell currExpected = reconcSheet.getRow(5).getCell(CellReference.convertColStringToIndex("F"));
		
		if (bnyTosht.getRow(4) == null) {
			bnyTosht.createRow(4);
		}
		
		
		String acct = "";
		double amount = 0;
		double amountDiff = 0;
		Optional<Integer> helixID = Optional.empty();
		String otherTag = "";
		
		Optional<Double> found = Optional.empty();
		String foundRef = "";
		String observedDesc = "";
		boolean check = false;
		boolean afterSweep = false;
		
		Cell currObserved = bnyTosht.getRow(4).getCell(CellReference.convertColStringToIndex("C")); // reset currObserved
		// iterate through the expected flows and the actual flows from the BNYM report. For each expected flow, it tries to find a matching actual flow based on criteria like account number, amount, description, Helix ID, etc.

//		// TONY //
//		// Create workbooks and sheets for expected and actual flows
//		Workbook expectedFlowsWorkbook = new XSSFWorkbook();
//		Sheet expectedFlowsSheet = expectedFlowsWorkbook.createSheet("Expected Flows");
//		Workbook actualFlowsWorkbook = new XSSFWorkbook();
//		Sheet actualFlowsSheet = actualFlowsWorkbook.createSheet("Actual Flows");
//
//		// Write headers for expected flows
//		Row expectedHeaderRow = expectedFlowsSheet.createRow(0);
//		expectedHeaderRow.createCell(0).setCellValue("Account");
//		expectedHeaderRow.createCell(1).setCellValue("Amount");
//		expectedHeaderRow.createCell(2).setCellValue("Helix ID");
//		expectedHeaderRow.createCell(3).setCellValue("Description");
//
//		// Write headers for actual flows
//		Row actualHeaderRow = actualFlowsSheet.createRow(0);
//		actualHeaderRow.createCell(0).setCellValue("Account");
//		actualHeaderRow.createCell(1).setCellValue("Amount");
//		actualHeaderRow.createCell(2).setCellValue("Description");
//		actualHeaderRow.createCell(3).setCellValue("Reference");
//
//		int expectedRowIndex = 1;
//		int actualRowIndex = 1;
//		// TONY



		while (!cellEmpty(currExpected)) {
//			// TONY
//			// Write expected flow to sheet
//			Row expectedRow = expectedFlowsSheet.createRow(expectedRowIndex++);
//			expectedRow.createCell(0).setCellValue(acct);
//			expectedRow.createCell(1).setCellValue(amount);
//			if (helixID.isPresent()) {
//				expectedRow.createCell(2).setCellValue(helixID.get());
//			}
//			expectedRow.createCell(3).setCellValue(otherTag);
//			//TONY


			found = Optional.empty();
			foundRef = "";
			observedDesc = "";
			check = false;
			afterSweep = false; 
			if (cellEmpty(currExpected.getRow().getCell(CellReference.convertColStringToIndex("B")))) {
				acct = currExpected.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue();
				amount = currExpected.getRow().getCell(CellReference.convertColStringToIndex("D")).getNumericCellValue();
			} else {
				acct = currExpected.getRow().getCell(CellReference.convertColStringToIndex("B")).getStringCellValue();
				amount = -1 * currExpected.getRow().getCell(CellReference.convertColStringToIndex("D")).getNumericCellValue();
			}
			if (cellEmpty(currExpected.getRow().getCell(CellReference.convertColStringToIndex("E")))) {
				helixID = Optional.empty();
			} else {
				helixID = Optional.of((int) currExpected.getRow().getCell(CellReference.convertColStringToIndex("E")).getNumericCellValue());
			}
			
			otherTag = currExpected.getRow().getCell(CellReference.convertColStringToIndex("F")).getStringCellValue();
			
			try {
				if (otherTag.substring(0, 7).equals("HXSWING")) {
					helixID = Optional.empty();
				}
			} catch(Exception excep) {}
			
			double marginAmountThisCP = 0;
			// net together all margin
			if (otherTag.substring(otherTag.lastIndexOf(' ') + 1).toUpperCase().equals("MARGIN")) {
				String cpName = "";
				if (otherTag.toUpperCase().contains("RECEIVE RETURNED")) {
					cpName = otherTag.substring(17, otherTag.lastIndexOf(' ')).toUpperCase();
				} else {
					cpName = otherTag.substring(otherTag.indexOf(' ') + 1, otherTag.lastIndexOf(' ')).toUpperCase();
				}
				
				Cell currMrgn = reconcSheet.getRow(5).getCell(CellReference.convertColStringToIndex("F"));
				while (!cellEmpty(currMrgn)) {
					boolean sameCP = false;
					try {
						String thisDesc = currMrgn.getStringCellValue();
						if (thisDesc.substring(thisDesc.lastIndexOf(' ') + 1).toUpperCase().equals("MARGIN")) {
							if (thisDesc.toUpperCase().contains("RECEIVE RETURNED")) {
								sameCP = cpName.equals(thisDesc.substring(17, thisDesc.lastIndexOf(' ')).toUpperCase());
							} else {
								sameCP = cpName.equals(thisDesc.substring(thisDesc.indexOf(' ') + 1, thisDesc.lastIndexOf(' ')).toUpperCase());
							}
						}
											
						if (sameCP) {
							if (!cellEmpty(currMrgn.getRow().getCell(CellReference.convertColStringToIndex("B")))) {
								if (currMrgn.getRow().getCell(CellReference.convertColStringToIndex("B")).getStringCellValue().equals(acct)) {
									marginAmountThisCP -= currMrgn.getRow().getCell(CellReference.convertColStringToIndex("D")).getNumericCellValue();
								}
							} else if (!cellEmpty(currMrgn.getRow().getCell(CellReference.convertColStringToIndex("C")))) {
								if (currMrgn.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue().equals(acct)) {
									marginAmountThisCP += currMrgn.getRow().getCell(CellReference.convertColStringToIndex("D")).getNumericCellValue();
								}
							}
						}
					} catch (Exception excp) {}
					
					if (reconcSheet.getRow(currMrgn.getRowIndex() + 1) == null) {
						break;
					}
					currMrgn = reconcSheet.getRow(currMrgn.getRowIndex() + 1).getCell(CellReference.convertColStringToIndex("F"));
				}
				// amount = marginAmountThisCP;
			}
			
			currObserved = bnyTosht.getRow(4).getCell(CellReference.convertColStringToIndex("C")); // reset currObserved
			while (found.isEmpty() && !cellEmpty(currObserved)) {
				

				boolean acctIsSame = ("" + currObserved.getStringCellValue().substring(0, 6)).equals(acct); // IMR level ignores 8400 vs 8401 
				acctIsSame = acctIsSame || (acct.equals("277540") && currObserved.getStringCellValue().substring(0, 6).equals("223031")); // just for now, hacky way to enable ECL account. hardcoded
				if (acctIsSame) {
					
					// first determine whether sweep has occured yet * in this account * INVARIANT: assumes BNY report ordered by time
					// aftersweep toggler will be ignored if dividend receipt, since that seems to break the ordering invariant
					try {
						if (currObserved.getRow().getCell(CellReference.convertColStringToIndex("S")).getStringCellValue().equals("STIF LOCATIONS") && !currObserved.getRow().getCell(CellReference.convertColStringToIndex("H")).getStringCellValue().equals("DIVIDEND")) {
							afterSweep = true;
						}
					} catch (Exception e) {}
					
					observedDesc = "";
					try {
						observedDesc = currObserved.getRow().getCell(CellReference.convertColStringToIndex("E")).getStringCellValue();
					} catch (IllegalStateException exc) {
						observedDesc = "" + ((int) currObserved.getRow().getCell(CellReference.convertColStringToIndex("E")).getNumericCellValue());
					} catch (Exception othE) {}
					
					if (helixID.isPresent()) {
						try {
							String observedID = "";
							for (int ci = 0; ci < observedDesc.trim().length(); ci++) {
								if (Character.isDigit(observedDesc.trim().charAt(ci))) {
									observedID += Character.toString(observedDesc.trim().charAt(ci));
								} else {
									break;
								}
							}
							if (Integer.parseInt(observedID) == helixID.get()) {
								check = true;
							}
						} catch (Exception excc) {}
						
					}
					
					check = check || (helixID.isEmpty() && (otherTag.equals(observedDesc)));
					try {
						check = check || (helixID.isEmpty() && (otherTag.replaceAll("_", " ").equals(observedDesc.substring(0, observedDesc.lastIndexOf(" "))))); // eg PO AGNC vs PO AGNC 1
					} catch (Exception e) {}
					
					try {
						String obsType = currObserved.getRow().getCell(CellReference.convertColStringToIndex("H")).getStringCellValue();
						if (otherTag.substring(otherTag.lastIndexOf(' ') + 1).toUpperCase().equals("MARGIN")) {
							String cpName = "";
							if (otherTag.toUpperCase().contains("RECEIVE RETURNED")) {
								cpName = otherTag.substring(17, otherTag.lastIndexOf(' ')).toUpperCase();
							} else {
								cpName = otherTag.substring(otherTag.indexOf(' ') + 1, otherTag.lastIndexOf(' ')).toUpperCase();
							}
							cpName = cpName.replaceAll("_", " ");
							// first check if exact match (in case not all one wire, happens sometimes eg for PNI receipt
							if (amount < 0) {
								try {
									check = check || observedDesc.toUpperCase().substring(0,5 + cpName.length()).equals("MRGN " + cpName); // just on amount for now, doesn't quite match it.
								} catch (Exception e) {}
								check = check || (Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue() - amount) <= 0.05);
							} else if (amount > 0) {
								if (currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue() >= 0) {
									//check = check || (cellEmpty(currObserved.getRow().getCell(CellReference.convertColStringToIndex("B"))) && (Math.abs(Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue()) - Math.abs(amount)) <= 0.01));
									check = check || (Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue() - amount) <= 0.05);
								}
							}
							
							if (!check) {
								// then check if net margin amount vs this counterparty
								if (marginAmountThisCP > 0) {
									if (obsType.equals("CASH DEPOSIT")) {
										check = check || (Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue() - marginAmountThisCP) <= 0.05);
									}
								} else {
									check = check || (Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue() - marginAmountThisCP) <= 0.05);
								}
							}

						}
						
						if (obsType.equals("DIVIDEND")) {
							check = check || ( cellEmpty(currObserved.getRow().getCell(CellReference.convertColStringToIndex("B"))) && (Math.abs(Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue()) - Math.abs(amount)) <= 0.03));
						}
					} catch(Exception excep) {}
					
					// if still not matched, check if incoming pairoff (outgoing already accounted for)
					try {
						if (!check) {
							if (otherTag.substring(0, 3).toUpperCase().equals("PO ")) { // pairoff format
								if (amount > 0) { // amount < 0 should be caught by description
									String obsType = currObserved.getRow().getCell(CellReference.convertColStringToIndex("H")).getStringCellValue();
									if (obsType.equals("CASH DEPOSIT")) {
										//check = check || (cellEmpty(currObserved.getRow().getCell(CellReference.convertColStringToIndex("B"))) && (Math.abs(Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue()) - Math.abs(amount)) <= 0.01));
										check = check || (Math.abs(Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue()) - Math.abs(amount)) <= Math.abs(pairoffDiffThreshold));
									}
								}
								
								
							}
						}
					} catch (Exception e) {}
					
					// if still not matched, just check based on amount.
					try {
						if (!check) {
							if (amount > 0) { // amount < 0 should be caught elsewhere
								String obsType = currObserved.getRow().getCell(CellReference.convertColStringToIndex("H")).getStringCellValue();
								if (obsType.equals("CASH DEPOSIT")) {
									check = check || (Math.abs(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue() - amount) <= 0.01);
								}
							}		
						}
					} catch (Exception e) {}
					
					if (check) {

//						// TONY
//						// Write actual flow to sheet
//						Row actualRow = actualFlowsSheet.createRow(actualRowIndex++);
//						actualRow.createCell(0).setCellValue(acct);
//						actualRow.createCell(1).setCellValue(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue());
//						actualRow.createCell(2).setCellValue(observedDesc);
//						actualRow.createCell(3).setCellValue(foundRef);
//						// TONY


						found = Optional.of(currObserved.getRow().getCell(CellReference.convertColStringToIndex("N")).getNumericCellValue());
						try {
							foundRef = currObserved.getRow().getCell(CellReference.convertColStringToIndex("T")).getStringCellValue();
						} catch (IllegalStateException e) {
							foundRef = "" + currObserved.getRow().getCell(CellReference.convertColStringToIndex("T")).getNumericCellValue();
						}
						currObserved.getRow().getCell(CellReference.convertColStringToIndex("B"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(otherTag);
					}
				}
				
				if (bnyTosht.getRow(currObserved.getRowIndex() + 1) == null) {
					break;
				}
				currObserved = bnyTosht.getRow(currObserved.getRowIndex() + 1).getCell(CellReference.convertColStringToIndex("C")); // move currObserved
			}

//			// TONY
//			// Save expected flows to file
//			FileOutputStream expectedOutputStream = new FileOutputStream("Expected Flows.xlsx");
//			expectedFlowsWorkbook.write(expectedOutputStream);
//			expectedOutputStream.close();
//			expectedFlowsWorkbook.close();
//
//			// Save actual flows to file
//			FileOutputStream actualOutputStream = new FileOutputStream("Actual Flows.xlsx");
//			actualFlowsWorkbook.write(actualOutputStream);
//			actualOutputStream.close();
//			actualFlowsWorkbook.close();
//			//TONY


			if (found.isPresent()) {
				currExpected.getRow().getCell(CellReference.convertColStringToIndex("H"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(found.get());
				currExpected.getRow().getCell(CellReference.convertColStringToIndex("I"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(Math.abs(found.get()) - Math.abs(amount));
				try {
					if (Math.abs(found.get() - amount)>0.05 && otherTag.substring(otherTag.lastIndexOf(' ') + 1).toUpperCase().equals("MARGIN")) {
						currExpected.getRow().getCell(CellReference.convertColStringToIndex("I"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(Math.abs(found.get() - marginAmountThisCP));
					}
				} catch(Exception e) {}
				currExpected.getRow().getCell(CellReference.convertColStringToIndex("J"), MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(foundRef);
				try {
					this.getFundByBNYMAccountID(Integer.parseInt(acct)).declareSettle(rowToFlow.get(currExpected.getRowIndex()), afterSweep);
				} catch (Exception e) {
					System.out.println("Should be unreachable.");
				}
			}
			
			if (reconcSheet.getRow(currExpected.getRowIndex() + 1) == null) {
				break;
			}
			currExpected = reconcSheet.getRow(currExpected.getRowIndex() + 1).getCell(CellReference.convertColStringToIndex("F"));
		}
				

		
		curr = null;
		Row currentRow = null;
		double prevBalance = 0;
		double currFlow = 0;
		Transaction currTran;
		double beginningCash = 0;
		double beginningSweep = 0;
		
		Sheet sht;
		
		HashMap<String, String> cashReconcNameToAcct = new HashMap<String, String>();
		cashReconcNameToAcct.put("C", "MAIN");
		cashReconcNameToAcct.put("F", "MARGIN");
		
		Flow currAcctFlow;
		
		for (Fund f : funds) {
			
			sht = expectedCFTemplate.getSheet(f.getFundID() + " Cash Table");
			List<Transaction> vDateTransactions = f.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
			Set<Flow> unsettledFlows = new HashSet<Flow>();
			int cRow = 4;
			currentRow = sht.getRow(cRow);
			if (currentRow == null) {
				currentRow = sht.createRow(cRow);
			}
			
			currentRow.getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("Beginning balance");
			
			for (String colLetter: cashReconcNameToAcct.keySet()) {

				beginningCash = f.getAcctByName(cashReconcNameToAcct.get(colLetter)).getInitialPosition(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
				beginningSweep = f.getAcctByName(cashReconcNameToAcct.get(colLetter)).getInitialPosition(f.getSweepVehicle(), HoldingStatus.AVAILABLE);
				currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 1).setCellValue(beginningCash);
				currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 2).setCellValue(beginningSweep);
			}
			
			// increment row
			cRow++;
			currentRow = sht.getRow(cRow);
			if (currentRow == null) {
				currentRow = sht.createRow(cRow);
			}
			
			for (int i = 0; i < vDateTransactions.size(); i++) {
				currTran = vDateTransactions.get(i);
				currentRow.getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(currTran.getDescription());
				// set flows
				for (String colLetter: cashReconcNameToAcct.keySet()) {
					currAcctFlow = currTran.getFlowToAccount(f.getAcctByName(cashReconcNameToAcct.get(colLetter)), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
					if (currAcctFlow != null) {
						if (currAcctFlow.hasSettled().isEmpty()) {
							// if not yet settled
							unsettledFlows.add(currAcctFlow);
						} else {
							if (!currAcctFlow.hasSettled().get()) {
								// or if actively failing
								unsettledFlows.add(currAcctFlow);
							}
						}
						
						currFlow = currAcctFlow.getAmount();
					} else {
						currFlow = 0;
					}
					
					currentRow.getCell(CellReference.convertColStringToIndex(colLetter), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(currFlow);
					
					try {
						prevBalance = sht.getRow(currentRow.getRowNum() - 1).getCell(CellReference.convertColStringToIndex(colLetter) + 1).getNumericCellValue();
					} catch (Exception e) {
						prevBalance = 0;
					}
					currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 1, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(prevBalance + currFlow);
				
					try {
						prevBalance = sht.getRow(currentRow.getRowNum() - 1).getCell(CellReference.convertColStringToIndex(colLetter) + 2).getNumericCellValue();
					} catch (Exception e) {
						prevBalance = 0;
					}
					currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(prevBalance + 0);
				
				
				}
				
				// increment row
				cRow++;
				currentRow = sht.getRow(cRow);
				if (currentRow == null) {
					currentRow = sht.createRow(cRow);
				}
			}
			 
			for (Flow badFl : unsettledFlows) {
				
				if (badFl.hasSettled().isEmpty()) {
					// unsettled
					currentRow.getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("UNSETTLED: " + badFl.getTransaction().getDescription());
				} else {
					currentRow.getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue("FAILING: " + badFl.getTransaction().getDescription());
				}
				
				// assume one to one
				String colLetter = "C";
				for (String s: cashReconcNameToAcct.keySet()) {
					if (cashReconcNameToAcct.get(s).equals(badFl.getAccount().getName())) {
						colLetter = s;
					}
				}
				
				currentRow.getCell(CellReference.convertColStringToIndex(colLetter), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(-badFl.getAmount());
				try {
					prevBalance = sht.getRow(currentRow.getRowNum() - 1).getCell(CellReference.convertColStringToIndex(colLetter) + 1).getNumericCellValue();
				} catch (Exception e) {
					prevBalance = 0;
				}
				currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 1, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(prevBalance - badFl.getAmount());
				
				try {
					prevBalance = sht.getRow(currentRow.getRowNum() - 1).getCell(CellReference.convertColStringToIndex(colLetter) + 2).getNumericCellValue();
				} catch (Exception e) {
					prevBalance = 0;
				}
				currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(prevBalance + 0);
			
				
				
				
				// increment row
				cRow++;
				currentRow = sht.getRow(cRow);
				if (currentRow == null) {
					currentRow = sht.createRow(cRow);
				}
			}
		}
		
		
		
		// end cash table part
		
		
		
		
		// save and close
		String now = new SimpleDateFormat("yyyyMMdd").format(valDate); // no timestamp in filepath, just date
		
		String fopStr = "BNYMCashReconc.xlsx";
		if (!sameDate(valDate,new Date())) {
//			fopStr = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\BNYMCashReconc_" + now + ".xlsx";
			fopStr = "S:\\Mandates\\Operations\\Daily Reconciliation\\Tony\\Output\\BNYMCashReconc_" + now + ".xlsx";
		}
		FileOutputStream fop = new FileOutputStream(fopStr); 
		expectedCFTemplate.write(fop);
		//expectedCFTemplate.write(fop);	
		expectedCFTemplate.close();
		fop.close();
		fistemp.close();
		
		for (Fund fsw: sweepDetected) {
			System.out.println("Sweep detected in " + fsw.getFundID() + " so sweeping..."); // TODO should do this before intraday reconc so can compare but ok.
			fsw.MMSweep(); // Important: tool must anticipate sweep amount instead of just taking it from BNY so it can allocate
		}
		
		// regarrdless of whether sweep occured in a fund, still set all trades not yet settled failing if eod
		for (Fund fnys: funds) {
			if (!sweepDetected.contains(fnys)) { // else already did it at mmsweep time
				fnys.setTradesNotYetSettledFailing();
			}
		}
		
		// check all allocated
		if (!allAllocated()) {
			System.out.println("Some trades not yet allocated properly.");
		}
		
		
		
		// just for testing
		//System.out.println(this.getFundByName("PRIME").getEODPositionInSeriesInAcct("MONTHLY", "MAIN", this.getFundByName("PRIME").getSweepVehicle(), HoldingStatus.AVAILABLE));
	}

	public void saveCFsToXLSX(String yesterdayPath, String savePath, String backupPath, boolean projectingSoSettleAll) throws IOException {
		FileInputStream fis = new FileInputStream(new File(yesterdayPath));
		Workbook currWB = new XSSFWorkbook(fis);
		
		Cell curr;
		Row currentRow;
		int cRow = 7;
		double prevBalance = 0;
		double currFlow = 0;
		Transaction currTran;
		
		Sheet sht; //= currWB.getSheet("Main");
		int shtI = 1;
		sht = currWB.getSheetAt(shtI);
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/d/yyyy");
		
		while (sht != null) {
			Fund f = getFundByName(sht.getRow(1).getCell(CellReference.convertColStringToIndex("C")).getStringCellValue());
			String seriesName = "";
			if (!cellEmpty(sht.getRow(2).getCell(CellReference.convertColStringToIndex("C")))) {
				seriesName = sht.getRow(2).getCell(CellReference.convertColStringToIndex("C")).getStringCellValue();
			}
			List<Transaction> vDateTransactions = null;
			
			if (seriesName.equals("")) {
				// if fundwide
				vDateTransactions = f.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
				
				
			} else {
				vDateTransactions = f.getTransactionsInSeries(seriesName, HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
			}
			
			// find first available row
			cRow = 7;
			
			currentRow = sht.getRow(cRow);
			if (currentRow == null) {
				currentRow = sht.createRow(cRow);
			}
			
			while (!cellEmpty(currentRow.getCell(CellReference.convertColStringToIndex("B")))) {
				cRow++;
				currentRow = sht.getRow(cRow);
				if (currentRow == null) {
					currentRow = sht.createRow(cRow);
				}
			}
			
			for (int i = 0; i < vDateTransactions.size(); i++) {
				currTran = vDateTransactions.get(i);
				currentRow.getCell(CellReference.convertColStringToIndex("B"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(dateFormat.format(valDate));
				// set flows
				for (String colLetter: colToAcct.keySet()) {
					if (projectingSoSettleAll) {
						currFlow = currTran.getFlowAmountToAccount(f.getAcctByName(colToAcct.get(colLetter)), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
					} else {
						currFlow = currTran.getSettledFlowAmountToAccount(f.getAcctByName(colToAcct.get(colLetter)), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
					}
					
					currentRow.getCell(CellReference.convertColStringToIndex(colLetter), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(currFlow);
					try {
						prevBalance = sht.getRow(currentRow.getRowNum() - 1).getCell(CellReference.convertColStringToIndex(colLetter) + 1).getNumericCellValue();
					} catch (Exception e) {
						prevBalance = 0;
					}
					currentRow.getCell(CellReference.convertColStringToIndex(colLetter) + 1, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(prevBalance + currFlow);
				}
				
				currentRow.getCell(CellReference.convertColStringToIndex("M"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(currTran.getDescription());
				
				// increment row
				cRow++;
				currentRow = sht.getRow(cRow);
				if (currentRow == null) {
					currentRow = sht.createRow(cRow);
				}
			}
			
			// sweep balances
			cRow = 7;
			currentRow = sht.getRow(cRow);
			
			while (!cellEmpty(currentRow.getCell(CellReference.convertColStringToIndex("O")))) {
				cRow++;
				currentRow = sht.getRow(cRow);
				if (currentRow == null) {
					currentRow = sht.createRow(cRow);
				}
			}
			
			currentRow.getCell(CellReference.convertColStringToIndex("O")).setCellValue(dateFormat.format(valDate));
			String acctName = "";
			for (int colnum = 15; colnum < 20; colnum++) {
				acctName = sht.getRow(6).getCell(colnum).getStringCellValue();
				if (seriesName.equals("")) {
					// if fundwide
					if (projectingSoSettleAll) {
						currentRow.getCell(colnum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(f.getAcctByName(acctName).getProjectedEODPosition(f.getSweepVehicle(), HoldingStatus.AVAILABLE));
					} else {
						currentRow.getCell(colnum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(f.getAcctByName(acctName).getEODPosition(f.getSweepVehicle(), HoldingStatus.AVAILABLE));
					}
					
				} else {
					if (projectingSoSettleAll) {
						currentRow.getCell(colnum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(f.getProjectedEODPositionInSeriesInAcct(seriesName, acctName, f.getSweepVehicle(), HoldingStatus.AVAILABLE));
					} else {
						currentRow.getCell(colnum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK).setCellValue(f.getEODPositionInSeriesInAcct(seriesName, acctName, f.getSweepVehicle(), HoldingStatus.AVAILABLE));
					}
				}
			}
			
			// increment sheet
			shtI++;
			try {
				sht = currWB.getSheetAt(shtI);
			} catch (Exception e) {
				break;
			}
		}
		
		System.out.println("Saving to file...");
		FileOutputStream save = new FileOutputStream(new File(savePath));
		currWB.write(save);
		save.close();
		if (backupPath != null) {
			save = new FileOutputStream(new File(backupPath));
			currWB.write(save);
			save.close();
		}
		currWB.close();	
		fis.close();	
	}

	public void setTradesNotYetSettledFailing() {
		for (Fund f: funds) {
			f.setTradesNotYetSettledFailing();
		}
		
	}

}
