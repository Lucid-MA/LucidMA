package com.jvulopas.cash_tracker;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AutoCashTracker {
	
	public static void main(String[] args) {
		System.out.println("Welcome. Running reconciler");
		try {
//			Date valDate = new Date();

			Date valDate = null; // Initialize valDate to null
			String customDateString = "2024-05-06"; // Custom date input in 'yyyy-MM-dd' format

			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				valDate = dateFormat.parse(customDateString); // Parse the custom date string
			} catch (ParseException e) {
				System.out.println("Error parsing the date: " + e.getMessage());
				return; // Exit if the date format is incorrect
			}


			//Date valDate = new SimpleDateFormat("MM/dd/yyyy").parse("11/03/2023");
			Court court = new Court("S:\\Mandates\\Operations\\Daily Reconciliation\\TrackerState.xlsx", valDate);
			//Court court = new Court("S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_20210921.xlsx", valDate);
			
			System.out.println("Fetching trade data from Helix.");
			court.connectToHelix();
			court.fetchTradeData();
			court.disconnectFromHelix();
			System.out.println("Helix connection is closed.");
			System.out.println("Fetching expected swings and wires."); // MUST do this AFTER fetching data from helix
			court.fetchManualMovements("S:\\Mandates\\Operations\\Daily Reconciliation\\Cash Blotter.xlsx", valDate);
			String bnyFile = "S:\\Mandates\\Funds\\Fund Reporting\\NEXEN Reports\\CashRecon_" + (new SimpleDateFormat("ddMMyyyy")).format(valDate) + ".xls";
			//String bnyFile = "C:\\Users\\jvulopas\\Desktop\\tmp_trash\\CashRecon_15042021.xls";
			File testFile = new File(bnyFile);
			if (!testFile.exists()) {
				System.out.println("going manual");
				bnyFile = "S:\\Mandates\\Funds\\Fund Reporting\\NEXEN Reports\\Archive\\CashRecon_" + (new SimpleDateFormat("ddMMyyyy")).format(valDate) + ".xls";
			}			
			System.out.println("Comparing against BNYM.");
			court.bnymCashReconc(bnyFile);
			System.out.println("Reconciliation file generated.");
	        // if intraday, save to current (open) file. at end of day another proc will overwrite the main state files
	        System.out.println("Saving state to intraday files...");
	        
			court.saveStateToXLSX("TrackerState Template.xlsx", "openTrackerState_test.xlsx", null);
			
			court.saveCFsToXLSX("S:\\Mandates\\Operations\\Daily Reconciliation\\CashFlows.xlsx", "openCashFlows_test.xlsx", null, false);
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
