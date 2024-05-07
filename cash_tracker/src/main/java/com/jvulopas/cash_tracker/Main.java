package com.jvulopas.cash_tracker;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

public class Main implements Runnable {

	public void run() {
		
		String dtIn = "";
		SimpleDateFormat dateForm = new SimpleDateFormat("MM/dd/yyyy");
		while (dtIn.equals("")) {
			dtIn = JOptionPane.showInputDialog(null,"Choose date", JOptionPane.PLAIN_MESSAGE);
			//dtIn = "07/14/2020";
			try {
				Date testDate = dateForm.parse(dtIn);
				if (testDate.compareTo(new Date()) > 0) {
					throw new Exception("");
				}
			} catch (Exception e) {
				dtIn = "";
			}
		}
		
		try {
			final Date valDate = dateForm.parse(dtIn);
			
	//		String customDate = "2020-05-01";
	//		try {
	//			final Date valDate = (new SimpleDateFormat("yyyy-MM-dd")).parse(customDate);
	//		} catch (ParseException e1) {
	//			final Date valDate = (new SimpleDateFormat("yyyy-MM-dd")).parse(customDate);
	//		}
	//		
			
			
			final String defaultStatusLabel = "Ready.";
			
			final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM dd");
			
			final JFrame frame = new JFrame("Lucid Cash & Securities Tracker");
			//frame.setLayout(new FlowLayout());
			String trackerStatePath = "S:\\Mandates\\Operations\\Daily Reconciliation\\TrackerState.xlsx";
			if(!dateForm.format(valDate).equals(dateForm.format(new Date()))) { // TODO if has already been created then use the yesterday file
				LocalDate prevD = valDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
				prevD = prevD.minusDays(1);
				trackerStatePath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_" + prevD.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + ".xlsx";
				File testFile = new File(trackerStatePath);
				int counter = 0;
				while (!testFile.exists() & counter < 10) {
					counter++;
					prevD = prevD.minusDays(1);
					trackerStatePath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_" + prevD.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + ".xlsx";
					testFile = new File(trackerStatePath);
				}
			}
			final String tStatePath = trackerStatePath;
			final Court court = new Court(tStatePath, valDate);
			JPanel bottom = new JPanel();
			bottom.setLayout(new BorderLayout());
			
			JPanel controlPanel = new JPanel();
			final JLabel generalStatusLabel = new JLabel("                                                         ");
			generalStatusLabel.setText(defaultStatusLabel);
			final JButton resetButton = new JButton("Restart");
			// TODO tab, one for flows today, another for beginning balances, flows, end balances list tables, another for query results https://www.programcreek.com/java-api-examples/javax.swing.table.DefaultTableModel
			resetButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					int check = JOptionPane.showConfirmDialog(null, "Are you sure you want to reset to BOD positions?", "Restart?", JOptionPane.YES_NO_OPTION);
			        if (check == JOptionPane.YES_OPTION) {
			        	generalStatusLabel.setText("Reloading fund info.");
			        	court.reset(tStatePath, valDate); // TODO make so choose date...
			        	generalStatusLabel.setText(defaultStatusLabel);
			        }	
				}	
			});
			
	
			final JButton fetchTradesButton = new JButton("Fetch Helix activity");
			
			fetchTradesButton.addActionListener(new ActionListener() {
				
				public void actionPerformed(ActionEvent e) {
					if (!court.connectedToHelix()) {
						JOptionPane.showMessageDialog(null, "Not connected to Helix.", "Helix", JOptionPane.ERROR_MESSAGE);
					} else {
						int check = JOptionPane.showConfirmDialog(null, "Fetch trading activity from Helix for " + simpleDateFormat.format(valDate)+ "?", "Restart?", JOptionPane.YES_NO_OPTION);
				        if (check == JOptionPane.YES_OPTION) {
				        	generalStatusLabel.setText("Querying Helix...");
				        	try {
				        		court.fetchTradeData();
				        	} catch (SQLException ex) {
				        		JOptionPane.showMessageDialog(null, "Error fetching data from Helix.", "Helix", JOptionPane.ERROR_MESSAGE);
				        	}
				        	generalStatusLabel.setText(defaultStatusLabel);
				        }	
					}
				}	
				
			});
			
			
			final JButton fetchManualMovementsButton = new JButton("Fetch swing & wire activity");
			
			fetchManualMovementsButton.addActionListener(new ActionListener() {
	
				public void actionPerformed(ActionEvent e) {
					try {
						generalStatusLabel.setText("Reloading additional flows.");
						court.fetchManualMovements("S:\\Mandates\\Operations\\Daily Reconciliation\\Cash Blotter.xlsx", valDate);     	
					} catch (Exception ex) {
						ex.printStackTrace();
						JOptionPane.showMessageDialog(null, "Error fetching manual movements.", "Error", JOptionPane.ERROR_MESSAGE);
					} finally {
						generalStatusLabel.setText(defaultStatusLabel);
					}
				}
				
				
			});
			
			
			final JButton disconnectFromHelixButton = new JButton("Disconnect from Helix");
			final JButton connectToHelixButton = new JButton("Connect to Helix");
			
			disconnectFromHelixButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					if (!court.connectedToHelix()) {
						JOptionPane.showMessageDialog(null, "Not connected to Helix.", "Helix", JOptionPane.PLAIN_MESSAGE);
					} else {
						generalStatusLabel.setText("Closing Helix connection...");
			        	try {
			        		court.disconnectFromHelix();
			        		connectToHelixButton.setVisible(true);
			        		disconnectFromHelixButton.setVisible(false);
			        		fetchTradesButton.setVisible(false);
			        		JOptionPane.showMessageDialog(null, "Helix connection closed.", "Helix", JOptionPane.PLAIN_MESSAGE);
			        	} catch (SQLException ex) {
			        		JOptionPane.showMessageDialog(null, "Error encountered disconnecting from Helix.", "Helix", JOptionPane.ERROR_MESSAGE);
			        	}
			        	
			        	generalStatusLabel.setText(defaultStatusLabel);
					}
				}	
			});
			
			
			
			connectToHelixButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					if (court.connectedToHelix()) {
						JOptionPane.showMessageDialog(null, "Already connected to Helix.", "Helix", JOptionPane.PLAIN_MESSAGE);
					} else {
						generalStatusLabel.setText("Connecting to Helix...");
			        	try {
			        		court.connectToHelix(); // TODO make so choose date...
			        		fetchTradesButton.setVisible(true);
			        		disconnectFromHelixButton.setVisible(true);
			        		connectToHelixButton.setVisible(false);
			        		JOptionPane.showMessageDialog(null, "Connected to Helix", "Helix", JOptionPane.PLAIN_MESSAGE);
			        		
			        		
			        	} catch (SQLException ex) {
			        		JOptionPane.showMessageDialog(null, "Error encountered connecting to Helix.", "Helix", JOptionPane.ERROR_MESSAGE);
			        	}
			        	
			        	generalStatusLabel.setText(defaultStatusLabel);
					}
				}	
			});
			
			final JButton saveToExcelButton = new JButton("Save to Excel");
			saveToExcelButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					generalStatusLabel.setText("Saving data...");
					try {
						// court.saveStateToXLSX("TrackerState Template.xlsx", "testwithmarginTrackerState.xlsx", "backupmargintestTrackerState.xlsx"); // for testing	
						if (HoldingsModel.sameDate(valDate, new Date())) {
							court.saveStateToXLSX("TrackerState Template.xlsx", "S:\\Mandates\\Operations\\Daily Reconciliation\\TrackerState.xlsx", "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_" + (new SimpleDateFormat("yyyyMMdd")).format(valDate) + ".xlsx");
							court.saveCFsToXLSX("S:\\Mandates\\Operations\\Daily Reconciliation\\CashFlows.xlsx", "S:\\Mandates\\Operations\\Daily Reconciliation\\CashFlows.xlsx","S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows_" + (new SimpleDateFormat("yyyyMMdd")).format(valDate) + ".xlsx", false);
							JOptionPane.showMessageDialog(null, "Cashflows saved to Excel.", "Excel", JOptionPane.PLAIN_MESSAGE);
						} else {
							LocalDate prevD = valDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
							prevD = prevD.minusDays(1);
							String cfPath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows_" + prevD.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + ".xlsx";
							File testFile = new File(cfPath);
							int counter = 0;
							while (!testFile.exists() & counter < 10) {
								counter++;
								prevD = prevD.minusDays(1);
								cfPath = "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows" + prevD.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + ".xlsx";
								testFile = new File(cfPath);
							}
							court.saveStateToXLSX("TrackerState Template.xlsx", "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_" + (new SimpleDateFormat("yyyyMMdd")).format(valDate) + ".xlsx", "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\TrackerState_" + (new SimpleDateFormat("yyyyMMdd")).format(valDate) + ".xlsx");
							court.saveCFsToXLSX(cfPath, "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows_" + (new SimpleDateFormat("yyyyMMdd")).format(valDate) + ".xlsx", "S:\\Mandates\\Operations\\Daily Reconciliation\\Historical\\CashFlows_" + (new SimpleDateFormat("yyyyMMdd")).format(valDate) + ".xlsx", false);
							
							try {
								String notifyStr = "(" + new SimpleDateFormat("E MM/dd/Y;k:m:s.S").format(new Date()) + "\";Save EOD Cash Tracker State\")";
								FileWriter fw = new FileWriter("S:\\Users\\J Vulopas\\batch_scripts\\BatchLogs.txt", true);
								BufferedWriter bout = new BufferedWriter(fw);
								bout.newLine();
								bout.append(notifyStr);
								bout.close();
								fw.close();
							} catch (Exception ioE) {
								System.out.println("Unable to notify BatchLogs...");
							}
							
							JOptionPane.showMessageDialog(null, "Cashflows saved to Excel.", "Excel", JOptionPane.PLAIN_MESSAGE);
						}
					} catch (Exception ex) {
						JOptionPane.showMessageDialog(null, "Error encountered saving to source.", "Error", JOptionPane.ERROR_MESSAGE);
					}
					generalStatusLabel.setText(defaultStatusLabel);
				}
			});
			
			
			final JButton bnymReconciliationButton = new JButton("Run BNYM Reconciliation");
			
			bnymReconciliationButton.addActionListener(new ActionListener() {
	
				public void actionPerformed(ActionEvent e) {
					try {
						generalStatusLabel.setText("Running BNYM reconciliation");
						String bnyFile = "S:\\Mandates\\Funds\\Fund Reporting\\NEXEN Reports\\CashRecon_" + (new SimpleDateFormat("ddMMyyyy")).format(valDate) + ".xls";
						File testFile = new File(bnyFile);
						if (!testFile.exists()) {
							bnyFile = "S:\\Mandates\\Funds\\Fund Reporting\\NEXEN Reports\\Archive\\CashRecon_" + (new SimpleDateFormat("ddMMyyyy")).format(valDate) + ".xls";
						}
						court.bnymCashReconc(bnyFile);
						JOptionPane.showMessageDialog(null, "Reconciliation file generated", "BNYM Reconciliation", JOptionPane.PLAIN_MESSAGE);    	
					} catch (Exception ex) {
						ex.printStackTrace();
						JOptionPane.showMessageDialog(null, "Error running BNYM reconciliation.", "Error", JOptionPane.ERROR_MESSAGE);
					} finally {
						generalStatusLabel.setText(defaultStatusLabel);
					}
				}
				
			});
			
			bnymReconciliationButton.setVisible(true);
			connectToHelixButton.setVisible(true);
			fetchTradesButton.setVisible(false);
			disconnectFromHelixButton.setVisible(false);
			controlPanel.add(resetButton);
			controlPanel.add(connectToHelixButton);
			controlPanel.add(fetchTradesButton);
			controlPanel.add(disconnectFromHelixButton);
			controlPanel.add(fetchManualMovementsButton); // must do this after fetching helix data TODO build in then
			controlPanel.add(bnymReconciliationButton);
			controlPanel.add(saveToExcelButton);
			
			bottom.add(controlPanel,BorderLayout.NORTH);
			bottom.add(generalStatusLabel, BorderLayout.CENTER);
			
			ImageIcon unScaledIcon = new ImageIcon("lucid_logo.png");
			double scaleFactor = 0.5;
			ImageIcon scaledIcon = new ImageIcon(unScaledIcon.getImage().getScaledInstance((int) (unScaledIcon.getIconWidth() * scaleFactor), (int) (unScaledIcon.getIconHeight() * scaleFactor), Image.SCALE_DEFAULT));
			JLabel lucidLogo = new JLabel();
			lucidLogo.setIcon(scaledIcon);
			frame.add(lucidLogo, BorderLayout.PAGE_START);
			frame.add(bottom, BorderLayout.PAGE_END);
			frame.add(court, BorderLayout.CENTER);
			//frame.setLocation(300, 300);
			//frame.setPreferredSize(new Dimension(1600, 800));
			frame.pack();
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			frame.setVisible(true);
		} catch (Exception e) {
			System.exit(0);
		}
	}
	
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Main());
	}

}