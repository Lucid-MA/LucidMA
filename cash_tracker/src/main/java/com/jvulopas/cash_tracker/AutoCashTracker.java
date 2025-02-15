package com.jvulopas.cash_tracker;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.DayOfWeek;
import java.time.format.DateTimeFormatter;
public class AutoCashTracker {
	
	public static void main(String[] args) {
		System.out.println("Welcome. Running reconciler");
		try {
			Date valDate = null;
			try {
				valDate = new SimpleDateFormat("yyyy-MM-dd").parse("2024-11-14");
			} catch (ParseException e) {
				System.out.println("Error parsing the date: " + e.getMessage());
				return;
			}

			LocalDate localValDate = valDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			LocalDate prevBusinessDate = localValDate.minusDays(1);
			while (prevBusinessDate.getDayOfWeek() == DayOfWeek.SATURDAY || prevBusinessDate.getDayOfWeek() == DayOfWeek.SUNDAY) {
				prevBusinessDate = prevBusinessDate.minusDays(1);
			}

			String prevDateString = prevBusinessDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

			Court court = new Court("S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_" +  prevDateString + ".xlsx", valDate);
//			Court court = new Court("S:\\Mandates\\Operations\\Daily Reconciliation\\TrackerState.xlsx", valDate);
//			Court court = new Court("S:\\Mandates\\Operations\\Daily Reconciliation\\Tony\\TrackerState.xlsx", valDate);

			System.out.println("Fetching trade data from Helix.");
			court.connectToHelix();
			court.fetchTradeData();
			court.disconnectFromHelix();
			System.out.println("Helix connection is closed.");
			System.out.println("Fetching expected swings and wires."); // MUST do this AFTER fetching data from helix
			court.fetchManualMovements("S:\\Mandates\\Operations\\Daily Reconciliation\\Cash Blotter.xlsx", valDate);
//			court.fetchManualMovements("S:\\Mandates\\Operations\\Daily Reconciliation\\Cash Blotter.xlsx", valDate);
//			String bnyFile = "S:\\Mandates\\Funds\\Fund Reporting\\NEXEN Reports\\CashRecon_" + localValDate.format(DateTimeFormatter.ofPattern("ddMMyyyy")) + ".xls";
			String bnyFile = "S:\\Mandates\\Operations\\Daily Reconciliation\\CashRecon_test.xls";
			File testFile = new File(bnyFile);
			if (!testFile.exists()) {
				System.out.println("going manual");
				bnyFile = "S:\\Mandates\\Funds\\Fund Reporting\\NEXEN Reports\\Archive\\CashRecon_" + localValDate.format(DateTimeFormatter.ofPattern("ddMMyyyy")) + ".xls";
			}			
			System.out.println("Comparing against BNYM.");
			court.bnymCashReconc(bnyFile);
			System.out.println("Reconciliation file generated.");
	        // if intraday, save to current (open) file. at end of day another proc will overwrite the main state files
	        System.out.println("Saving state to intraday files...");

			// Format the valDate as a string in the desired format (e.g., yyyyMMdd)
			String valDateString = new SimpleDateFormat("yyyyMMdd").format(valDate);
			// Update the savePath for OpenTrackerState
			String openTrackerStatePath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\openTrackerState_" + valDateString + ".xlsx";
			court.saveStateToXLSX("S:\\Mandates\\Operations\\Daily Reconciliation\\TrackerState Template.xlsx", openTrackerStatePath, null);

			// Update the savePath for OpenCashFlows
			String openCashFlowsPath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\openCashFlows_" + valDateString + ".xlsx";
			// Construct the prevPath dynamically based on the previous business date
			String prevPath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows_" + prevDateString + ".xlsx";

			court.saveCFsToXLSX(prevPath, openCashFlowsPath, null, false);
			//court.saveCFsToXLSX("S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows_20210921.xlsx", "openCashFlows.xlsx", null, false);
			
			System.out.println("State saved.");

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
