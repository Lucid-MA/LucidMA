package com.jvulopas.cash_tracker;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;

import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;

@SuppressWarnings("serial")
public class Court extends JPanel {
	private HoldingsModel model;
	
	private final JLabel currFundLabel; // also doubles as broad status label
	private final FundFlowTable table;
	private final JComboBox<Fund> fundDropdown;
	private final JButton fundChangeButton;
	private final JButton manualSettleButton;
	private final JComboBox<String> seriesDropdown;
	private JLabel currSeriesLabel;
	
	public static final Set<String> ctptiesSplittingPairoffsByDepository = new HashSet<String>(Arrays.asList("400CAP", "CTVA", "AGNC"));;
	
	private JLabel currSecurityLabel; // display balance of current security, defaults to cash.
	private final JTextField securityField;
	
	private final JComboBox<HoldingStatus> holdingStatusDropdown;
	
	private Fund currFund;
	
	private final Date valDate;
	
	// SQL connections for trade data
	private HelixConnection helix;
	private boolean connectedToHelix;
	
	private QueryResultTable tradeQueryTable, marginQueryTable, counterpartyCashTable;
	
	private PlainTable manualMovementsTable;
	
	// handle MM Sweep, reconcile, and save, (and edit) functionality.
	private final JButton mmSweepButton;
	
	public Court(String initFilepath, Date vDate) {
		super();
		
		this.valDate = vDate;
		connectedToHelix = false;
		currFundLabel = new JLabel("Loading cash & securities positions");
		currFundLabel.setText("Booting up...");
		currSeriesLabel = new JLabel("Displaying xxxxxxxxxxxxxxxx");
		currSeriesLabel.setText("Displaying fundwide");
		
		currSecurityLabel = new JLabel("Displaying activity for xxxxxxxxxxxxxxxx xxxxxxxxxxxxxxxxx");
		currSecurityLabel = new JLabel("Displaying activity for available cash");
		
		securityField = new JTextField(); // todo default cash value? todo add press enter action listener here, so will update right away
		securityField.setText(HoldingsModel.getCashRepresentation()); // todo eventually make recognize all different types of cash 
		securityField.setMaximumSize(new Dimension(Integer.MAX_VALUE, securityField.getMinimumSize().height));
		
		holdingStatusDropdown = new JComboBox<HoldingStatus>();
		holdingStatusDropdown.setMaximumSize(new Dimension(Integer.MAX_VALUE, holdingStatusDropdown.getMinimumSize().height));
		
		for (HoldingStatus s: HoldingStatus.values()) {
			holdingStatusDropdown.addItem(s);
		}
		
		table = new FundFlowTable(); 
		
		fundDropdown = new JComboBox<Fund>();
		fundDropdown.setVisible(false); // TODO remove this?
		fundDropdown.setMaximumSize(new Dimension(Integer.MAX_VALUE, fundDropdown.getMinimumSize().height));
		
		// MM Sweep
        mmSweepButton = new JButton("MM Sweep");
        
        /**
         * Must MM Sweep across all series, then if need to make adjustments can afterwards.
         */
        mmSweepButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
	        	Fund currFundSelection = table.getCurrFund();
	        	//Fund currFundSelection = fundDropdown.getItemAt(fundDropdown.getSelectedIndex());
	        	if (currFundSelection != null) {
		        	String[] options = new String[]{"All funds", "Just " + currFundSelection.getFundID(), "Cancel"};
					int check = JOptionPane.showOptionDialog(null, "MM Sweep for all funds or just " + currFundSelection.getFundID() + "?", 
							"MM Sweep", JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, null);
			        if (check == JOptionPane.YES_OPTION) {
			        	model.mmSweep(); // sweep all
			        	table.setVisible(false);
			    		table.reload();
			    		table.setVisible(true);	
			        	
			        } else if (check == JOptionPane.NO_OPTION) {
			        	currFundSelection.MMSweep();
			        	table.setVisible(false);
			    		table.reload();
			    		table.setVisible(true);	
			        }
	        	}
	        	
			}
        });
		
		seriesDropdown = new JComboBox<String>();
		seriesDropdown.setMaximumSize(new Dimension(Integer.MAX_VALUE, seriesDropdown.getMinimumSize().height));
		
		fundChangeButton = new JButton("Apply");
		fundChangeButton.setMaximumSize(new Dimension(Integer.MAX_VALUE, fundChangeButton.getMinimumSize().height));
		fundChangeButton.setAlignmentX(Component.CENTER_ALIGNMENT);
		fundChangeButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				Fund fund = fundDropdown.getItemAt(fundDropdown.getSelectedIndex());
				String security = HoldingsModel.securitizeString(securityField.getText(), fund);
				String seriesChoice = seriesDropdown.getItemAt(seriesDropdown.getSelectedIndex());
				HoldingStatus holdingStatus = holdingStatusDropdown.getItemAt(holdingStatusDropdown.getSelectedIndex());
				
				if (("Current fund: " + fund).equals(currFundLabel.getText())) {
					if (!(("Displaying " + seriesChoice).equals(currSeriesLabel.getText()))) {
						currSeriesLabel.setText("Displaying " + seriesChoice);
					}
				} else {
					seriesDropdown.removeAllItems();
					seriesDropdown.addItem("fundwide");
					for (String s: fund.getSeriesNames()) {
						seriesDropdown.addItem(s);
					}
					currSeriesLabel.setText("Displaying fundwide");
					currFundLabel.setText("Current fund: " + fund);
					seriesChoice = "fundwide";
				}
				
				// available cash is special case for the jlabel
				if (security.equals(HoldingsModel.getCashRepresentation()) && holdingStatus == HoldingStatus.AVAILABLE) {
					if (!currSecurityLabel.getText().equals("Displaying activity for available cash")) {
						currSecurityLabel.setText("Displaying activity for available cash");
					}
				} else if (!currSecurityLabel.getText().equals("Displaying activity for " + security + " " + holdingStatus)) {
					currSecurityLabel.setText("Displaying activity for " + security + " " + holdingStatus);
				}
				//System.out.println(table.getZoomLabelInfo());
				table.set(fund, valDate, security, holdingStatus, seriesChoice); // better to make all react to same change in state a la value controller but this click event is only way state changes so OK
			}

//			public void actionPerformed(ActionEvent e) {
//				Fund fund = fundDropdown.getItemAt(fundDropdown.getSelectedIndex());
//				String security = HoldingsModel.securitizeString(securityField.getText(), fund);
//				String seriesChoice = seriesDropdown.getItemAt(seriesDropdown.getSelectedIndex());
//				
//				HoldingStatus holdingStatus = holdingStatusDropdown.getItemAt(holdingStatusDropdown.getSelectedIndex());
//				
//				if (("Current fund: " + fund).equals(currFundLabel.getText())) {
//					if (!(("Displaying " + seriesChoice).equals(currSeriesLabel.getText()))) {
//						currSeriesLabel.setText("Displaying " + seriesChoice);
//						table.setSeries(seriesChoice);
//					}
//				} else {
//					seriesDropdown.removeAllItems();
//					seriesDropdown.addItem("fundwide");
//					for (String s: fund.getSeriesNames()) {
//						seriesDropdown.addItem(s);
//					}
//					currSeriesLabel.setText("Displaying fundwide");
//					currFundLabel.setText("Current fund: " + fund);
//					table.setFund(fund, valDate, security, holdingStatus, seriesChoice); // better to make all react to same change in state a la value controller but this click event is only way state changes so OK
//				}
//				
//				// available cash is special case for the jlabel
//				if (security.equals(HoldingsModel.getCashRepresentation()) && holdingStatus == HoldingStatus.AVAILABLE) {
//					if (!currSecurityLabel.getText().equals("Displaying activity for available cash")) {
//						currSecurityLabel.setText("Displaying activity for available cash");
//						table.setSecurity(security, holdingStatus);
//					}
//				} else if (!currSecurityLabel.getText().equals("Displaying activity for " + security + " " + holdingStatus)) {
//					currSecurityLabel.setText("Displaying activity for " + security + " " + holdingStatus);
//					table.setSecurity(security, holdingStatus);
//				}
//			}
			
		});
		
		// TODO have warn if series allocs wrong
		
//		// TODO instead of apply button make every jcomponent affect independently?
//		// for now jsut series because don't want to display series not in fund (but okay to display securities not in fund, balance just = 0)
//		fundDropdown.addItemListener(new ItemListener() {
//
//			@SuppressWarnings("unchecked")
//			public void itemStateChanged(ItemEvent e) {
//				Fund selectedFund = (Fund) ((JComboBox<Fund>) e.getSource()).getSelectedItem();
//				if (!("Current fund: " + selectedFund.getFundID()).equals(currFundLabel.getText())) {
//					// then fund changed, so just change series dropdown
//					seriesDropdown.removeAllItems();
//					seriesDropdown.addItem("fundwide");
//					for (String s: ((Fund) ((JComboBox<Fund>) e.getSource()).getSelectedItem()).getSeriesNames()) {
//						seriesDropdown.addItem(s);
//					}
//				}
//			}
//			
//		});
//		
		manualSettleButton = new JButton("Manually settle.");
		manualSettleButton.setVisible(true);
		manualSettleButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int check = JOptionPane.showConfirmDialog(null, "Manually settle chosen flow?", "Manually declare settle?", JOptionPane.YES_NO_OPTION);
		        if (check == JOptionPane.YES_OPTION) {
		        	try {
		        		if (JOptionPane.showConfirmDialog(null, "Declare settled before sweep?", "Before or after sweep?", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
		        			table.declareSettle(false);
		        		} else {
		        			table.declareSettle(true);
		        		}
		        	} catch (Exception ex) {
		        		JOptionPane.showMessageDialog(null, "Error settling chosen flow.", "Manual settlement.", JOptionPane.ERROR_MESSAGE);
		        	}
		        }	
			}
			
		});
		
		this.add(manualSettleButton);
		
		tradeQueryTable = new QueryResultTable();
		marginQueryTable = new QueryResultTable();
		counterpartyCashTable = new QueryResultTable();
		manualMovementsTable = new PlainTable();
        
		JTabbedPane tabbedPane = new JTabbedPane();
        
		
		
		JPanel mainControlPanel = new JPanel();
		mainControlPanel.setLayout(new BoxLayout(mainControlPanel, BoxLayout.Y_AXIS));
		mainControlPanel.setAlignmentX(LEFT_ALIGNMENT);
		mainControlPanel.add(fundDropdown);
        mainControlPanel.add(seriesDropdown);
        mainControlPanel.add(securityField);
        mainControlPanel.add(holdingStatusDropdown);
        mainControlPanel.add(fundChangeButton);
        mainControlPanel.add(manualSettleButton);
		
        JPanel mainStatusDisplaysPanel = new JPanel(); // default to flow layout
        mainStatusDisplaysPanel.add(currFundLabel);
        mainStatusDisplaysPanel.add(new SpacerLabel(Color.BLUE));
        mainStatusDisplaysPanel.add(currSecurityLabel);
        mainStatusDisplaysPanel.add(new SpacerLabel(Color.BLUE));
        mainStatusDisplaysPanel.add(currSeriesLabel);
        mainStatusDisplaysPanel.add(new SpacerLabel(Color.BLUE));
        mainStatusDisplaysPanel.add(mmSweepButton);
        
        
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new BorderLayout());
        
        
        
        mainPanel.add(mainStatusDisplaysPanel, BorderLayout.PAGE_START);
        mainPanel.add(mainControlPanel, BorderLayout.LINE_START);
        mainPanel.add(table, BorderLayout.CENTER);
        
        tabbedPane.addTab("main", null, mainPanel,
                "Flow page");
     //   tabbedPane.setMnemonicAt(0, KeyEvent.VK_M);
        
        JComponent tradeQueryPanel = new JPanel();
        tradeQueryPanel.add(tradeQueryTable);
        
        JComponent marginQueryPanel = new JPanel();
        marginQueryPanel.add(marginQueryTable);
        
        JComponent manualMovementsPanel = new JPanel();
        manualMovementsPanel.add(manualMovementsTable);
        
        JComponent counterpartyCashPanel = new JPanel();
        counterpartyCashPanel.add(counterpartyCashTable);
        
        tabbedPane.addTab("trade query", null, tradeQueryPanel,
                "Trade query data");
      //  tabbedPane.setMnemonicAt(1, KeyEvent.VK_Q);
        
        tabbedPane.addTab("margin query", null, marginQueryPanel,
                "Margin query data");
       // tabbedPane.setMnemonicAt(2, KeyEvent.VK_M);
        
        tabbedPane.addTab("other movements", null, manualMovementsPanel, "Other movements data");
        
        tabbedPane.addTab("counterparty cash balances", null, counterpartyCashPanel, "Counterparty margin cash balances");
        add(tabbedPane);
        
        tabbedPane.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);
		
        helix = null; // not yet
        
        
		
		reset(initFilepath, valDate);
	}
	
	public void reset(String initFilepath, Date valDate) {
		currFundLabel.setText("Loading cash & securities positions");
		
		try {
			model = new HoldingsModel(initFilepath, valDate); //assumes valDate next day
		} catch (Exception e) {
			currFundLabel.setText("Error loading data...");
			JOptionPane.showMessageDialog(this,
				    "Error encountered on loading initial cash and securities positions. Exiting now...",
				    "Balance loading error",
				    JOptionPane.ERROR_MESSAGE);
			System.exit(0); // TODO dont exit asi 
		}
		
		fundDropdown.removeAllItems();
		for (String fn: model.getFundNames()) {
			Fund f = model.getFundByName(fn);
			fundDropdown.addItem(f);
		}
		
		seriesDropdown.removeAllItems();
		seriesDropdown.addItem("fundwide");
		
		for (String s: fundDropdown.getItemAt(0).getSeriesNames()) {
			seriesDropdown.addItem(s);
		}
		
		
		securityField.setText(HoldingsModel.getCashRepresentation());
		
		// default security dropdown and stts dropdown to available cash
		currSeriesLabel.setText("Displaying fundwide");
		// initialize table
		table.setVisible(false);
		table.setFund(fundDropdown.getItemAt(0), valDate); // defaults to available cash
		currFundLabel.setText("Current fund: " + fundDropdown.getItemAt(0));		
		table.setVisible(true);
		fundDropdown.setVisible(true);
		fundChangeButton.setVisible(true);
		
//		try {
//			connectToHelix();
//		} catch (SQLException e) {
//			
//		}
	}

	/**
	 * Reset Helix connection.
	 * @throws SQLException
	 */
	public void connectToHelix() throws SQLException {
		if (this.connectedToHelix()) {
			return;
		}
		helix = new HelixConnection();
		connectedToHelix = true; // will only enter if successful
	}

	public boolean connectedToHelix() {
		return connectedToHelix;
	}
	
	public void disconnectFromHelix() throws SQLException {
		if (!this.connectedToHelix()) {
			return;
		}
		helix.close();
		helix = null;
		connectedToHelix = false;
	}
	
	/**
	 * Assumes strict invariants about fund structure.
	 * 
	 * Set-up not perfectly MVC modular... some of these should be attributes of the HoldingsModel, not the court. 
	 * @throws SQLException
	 */
	public void fetchTradeData() throws SQLException {
		if (!this.connectedToHelix()) {
			return;
		}
		ResultSet helixData = helix.query(HelixConnection.TRADE_QUERY_ON_DATE(valDate));
		tradeQueryTable.populate(helixData);
		// populate holdings model with trade data
		// TODO make tradeQueryTable uneditable.
		// TODO but make flow table editable and have it inform holdings model?
		
		
		helixData.beforeFirst();
		
		HashSet<Integer> rollingOnValdate = new HashSet<Integer>();

		// just to populate rolling on date for pairoffs
		while (helixData.next()) {
			String tradeID = helixData.getString("action_id");
			try {
				tradeID = tradeID.substring(0, tradeID.indexOf(" "));
			} catch (Exception ee) {}
			String seriesName = helixData.getString("series");
			boolean fundHasOnlyOneSeries = helixData.getBoolean("is_also_master"); // hardwired in query for now (only prime has multiple series)
			boolean isBuySell = helixData.getBoolean("is_buy_sell");
			Date startDate = helixData.getDate("start_date");
			int rollOf = helixData.getInt("roll_of");
			
			if (seriesName.equals("MASTER") || fundHasOnlyOneSeries) {
				if (HoldingsModel.sameDate(valDate, startDate) && rollOf != 0) { // not & !isBuySell ; buySells still paired off
					rollingOnValdate.add(rollOf); // if starting on date and it's a roll store the piece it's a roll of
				}

			}
		}
		
		// put rolls being combined here.
		rollingOnValdate.add(131509);
		rollingOnValdate.add(131510);
		rollingOnValdate.add(131803);
		rollingOnValdate.add(134887);
		rollingOnValdate.add(131807);
		rollingOnValdate.add(134888);
		rollingOnValdate.add(131811);
		rollingOnValdate.add(134889);
		rollingOnValdate.add(132167);
		rollingOnValdate.add(132166);
		rollingOnValdate.add(132175);
		rollingOnValdate.add(134690);
		rollingOnValdate.add(132185);
		rollingOnValdate.add(135076);



		//rollingOnValdate.remove(61062);
		
		if (!rollingOnValdate.isEmpty()) {
			System.out.println("Determined rolling..........................");
			for (Integer x : rollingOnValdate) {
				System.out.println(x);
			}
		}

		
		HashSet<Integer> newRollsOnValDate = new HashSet<Integer>(); // need for series allocs of pairoffs
		
		helixData.beforeFirst(); // reset
		
		// since only doing flow model didn't create Trade UDT. good to do that in the long run
		while (helixData.next()) {
			String actionID = helixData.getString("action_id");
			String seriesName = helixData.getString("series");
			boolean fundHasOnlyOneSeries = helixData.getBoolean("is_also_master"); // hardwired in query for now (only prime has multiple series)
			boolean isBuySell = helixData.getBoolean("is_buy_sell");
			String fundID = helixData.getString("fund");
			int tradeTypeCode = helixData.getInt("trade_type");
			Date startDate = helixData.getDate("start_date");
			Date endDate = helixData.getDate("end_date");
			String security = helixData.getString("security");
			Double par = helixData.getDouble("quantity");
			Double money = helixData.getDouble("money");
			Double endMoney = helixData.getDouble("end_money");
			String counterparty = helixData.getString("counterparty");
			String depository = helixData.getString("depository");
			boolean naturalEndDate = helixData.getBoolean("set_to_term_on_date");
			int rollOf = helixData.getInt("roll_of");
			int tradeID = -1;
						
			try {
				tradeID = Integer.parseInt(actionID.substring(0, actionID.indexOf(" ")));	

			} catch (Exception ee) {}
			// upload master trade
			
			//naturalEndDate = true;
			if (seriesName.equals("MASTER") || fundHasOnlyOneSeries) {
				// in Helix, 0 is repo, 1 is reverse repo
				int modifier = 1;
				if (tradeTypeCode == 0) modifier = -1;
				if (HoldingsModel.sameDate(startDate, valDate) && rollOf != 0 && rollingOnValdate.contains(rollOf)) {
					// roll
					model.getFundByName(fundID).uploadPairoff(counterparty, -1 * modifier * Math.abs(money), depository);
					newRollsOnValDate.add(tradeID);
				//} else if (naturalEndDate && tradeID != -1 && rollingOnValdate.contains(tradeID)) {
				} else if (tradeID != -1 && rollingOnValdate.contains(tradeID)) {
					// roll
					model.getFundByName(fundID).uploadPairoff(counterparty, modifier * Math.abs(endMoney), depository);
				} else {	
					// not roll
					if (isBuySell) {
						// buy sell
						if (tradeTypeCode == 0) {
							if (HoldingsModel.sameDate(startDate, valDate)) {
								// sell
								model.getFundByName(fundID).uploadIncoming(actionID, "MAIN", HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, money, counterparty + "repo open (sell)");
								model.getFundByName(fundID).uploadOutgoing(actionID, "MAIN", security, HoldingStatus.AVAILABLE, par, counterparty + "repo open (sell)");
							} else {
								// buy back
								model.getFundByName(fundID).uploadOutgoing(actionID, "MAIN", HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, endMoney, counterparty + " repo close (buy)");
								model.getFundByName(fundID).uploadIncoming(actionID, "MAIN", security, HoldingStatus.AVAILABLE, par, counterparty + " repo close (buy)");
							}
						} else if (tradeTypeCode == 1) {
							if (HoldingsModel.sameDate(startDate, valDate)) {
								// buy
								model.getFundByName(fundID).uploadOutgoing(actionID, "MAIN", HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, money, counterparty + " reverse repo open (buy)");
								model.getFundByName(fundID).uploadIncoming(actionID, "MAIN", security, HoldingStatus.AVAILABLE, par, counterparty + " reverse repo open (buy)");
							} else {
								// sell back
								model.getFundByName(fundID).uploadIncoming(actionID, "MAIN", HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, endMoney, counterparty + " reverse repo close (sell)");
								model.getFundByName(fundID).uploadOutgoing(actionID, "MAIN", security, HoldingStatus.AVAILABLE, par, counterparty + " reverse repo close (sell)");
							}
						}
					} else {
						// normal, ie not buy sell and not roll
						if (tradeTypeCode == 0) {
							model.getFundByName(fundID).uploadRepo(actionID, HoldingsModel.sameDate(startDate, valDate), security, counterparty, par, HoldingsModel.sameDate(startDate, valDate) ? money : endMoney);
						} else if (tradeTypeCode == 1) {
							model.getFundByName(fundID).uploadReverseRepo(actionID, HoldingsModel.sameDate(startDate, valDate), security, counterparty, par, HoldingsModel.sameDate(startDate, valDate) ? money : endMoney);
						}
					}
					
					
				}
			}
			
			// if fund has only one series, is also an allocation
			if (!seriesName.equals("MASTER")) {
				BigDecimal used_alloc = helixData.getBigDecimal("used_alloc"); // again, assumes one alloc portion used across whole trade. not perfectly correct because this is not the allocator, sino indicates how things have been allocated (right or wrong), but fine assumption
				//System.out.println("court 431 " + used_alloc.doubleValue());
				
				int modifier = 1;
				if (tradeTypeCode == 0) modifier = -1;
				if (newRollsOnValDate.contains(tradeID)) {
					// roll
					model.getFundByName(fundID).uploadPairoffIntoSeries(seriesName, counterparty, -1 * modifier * Math.abs(money), depository);
				//} else if (naturalEndDate && tradeID != -1 && rollingOnValdate.contains(tradeID)) {
				} else if (tradeID != -1 && rollingOnValdate.contains(tradeID)) {
					// roll
					model.getFundByName(fundID).uploadPairoffIntoSeries(seriesName, counterparty, modifier * Math.abs(endMoney), depository);
				} else {
					// not roll
					model.getFundByName(fundID).allocate(seriesName, actionID, used_alloc.doubleValue());
					try {
						model.getFundByName(fundID).uploadUsedAlloc(seriesName, tradeID, used_alloc.doubleValue());
					} catch (Exception e) {
						System.out.println("Could not properly store allocation of " + tradeID + " in series " + seriesName + " in fund " + fundID);
					}
				}
			}
		}
		
		model.transactPairoffs();
		
		// populate holdings model with net cash by counterparty
		ResultSet counterpartyNetCashData = helix.query(HelixConnection.NET_CASH_BY_COUNTERPARTY(valDate));
		counterpartyCashTable.populate(counterpartyNetCashData);
		counterpartyNetCashData.beforeFirst();
		while (counterpartyNetCashData.next()) {
			String fundID = counterpartyNetCashData.getString("fund");
			String counterparty = counterpartyNetCashData.getString("acct_number");
			String seriesName = counterpartyNetCashData.getString("ledgername").toUpperCase();
			boolean fundHasOnlyOneSeries = counterpartyNetCashData.getBoolean("is_also_master"); // TODO hardwired in query for now (only prime has multiple series)
			Double amount = counterpartyNetCashData.getDouble("net_cash");
			Double activityToday = counterpartyNetCashData.getDouble("activity");
			if (seriesName.equals("MASTER") || fundHasOnlyOneSeries) {
				model.getFundByName(fundID).uploadCounterpartyCashBalance(counterparty, amount);
				model.getFundByName(fundID).uploadCounterpartyCashActivity(counterparty, activityToday);
			}
			// if fund has only one series, is also an allocation
			if (!seriesName.equals("MASTER")) {
				model.getFundByName(fundID).allocateCounterpartyCashBalance(seriesName, counterparty, amount);
				model.getFundByName(fundID).allocateCounterpartyCashActivity(seriesName, counterparty, activityToday);
			}
			
		}
				
		// populate holdings model with margin data
		ResultSet helixMarginData = helix.query(HelixConnection.TRADESFREE_QUERY_ON_DATE(valDate));
		marginQueryTable.populate(helixMarginData);
		helixMarginData.beforeFirst();
		while (helixMarginData.next()) {
			String actionID = helixMarginData.getString("action_id");
			if (actionID.equals("32939 TRANSMITTED")) continue;
			String seriesName = helixMarginData.getString("series");
			boolean fundHasOnlyOneSeries = helixMarginData.getBoolean("is_also_master"); // hardwired in query for now (only prime has multiple series)
			
			String fundID = helixMarginData.getString("fund");
			int tradeTypeCode = helixMarginData.getInt("trade_type");
			Date startDate = helixMarginData.getDate("start_date");
			Date closeDate = helixMarginData.getDate("close_date");
			String security = helixMarginData.getString("security");
			Double amount = helixMarginData.getDouble("amount");
			String counterparty = helixMarginData.getString("counterparty");
			
			if (seriesName.equals("MASTER") || fundHasOnlyOneSeries) {
				// in Helix, 22 is repofree, 23 is reversefree
				if (tradeTypeCode == 22) {
					if (security.equals(HoldingsModel.getCashRepresentation())) {
						model.getFundByName(fundID).uploadOutgoingMargin(actionID, security, HoldingsModel.sameDate(startDate, valDate), counterparty, Math.abs(amount));
					}
				} else if (tradeTypeCode == 23) {
					if (security.equals(HoldingsModel.getCashRepresentation())) {
						model.getFundByName(fundID).uploadIncomingMargin(actionID, security, HoldingsModel.sameDate(startDate, valDate), counterparty, Math.abs(amount));
					} else {
						try {
							if (security.substring(0, 3).equals("PNI")) {
								// TODO PNI?
							}
						} catch (Exception e) {}
						
					}
				}
			}
			// if fund has only one series, is also an allocation
			if (!seriesName.equals("MASTER")) {
				if (security.equals(HoldingsModel.getCashRepresentation())) {
					Double used_alloc = helixMarginData.getDouble("used_alloc"); // again, assumes one alloc portion used across whole trade. not perfectly correct because this is not the allocator, sino indicates how things have been allocated (right or wrong), but fine assumption
					model.getFundByName(fundID).allocate(seriesName, actionID, used_alloc);
					try {
						model.getFundByName(fundID).uploadUsedAlloc(seriesName, Integer.parseInt(actionID.substring(0, actionID.indexOf(' '))), used_alloc.doubleValue());
					} catch (Exception e) {
						System.out.println("Could not properly store allocation of margin trade " + actionID + " in series " + seriesName + " in fund " + fundID);
					}
					if (tradeTypeCode == 22 || tradeTypeCode == 23) {
						// here, affects the margin account and there are relevant swings
						
						String helixID = actionID.substring(0, actionID.indexOf(" "));
						String swingID = "HXSWING" + helixID;
						if (!HoldingsModel.sameDate(startDate, valDate)) {
							swingID = swingID + "CLS";
						}
						model.getFundByName(fundID).allocateTradeBasedSwingOrWire(seriesName,swingID, Optional.of(Integer.parseInt(helixID)));
					
					}
				} else {
					// TODO PNI?
				}
			}
			
		}
	
		
		
		
		table.setVisible(false);
		table.reload();
		table.setVisible(true);
	}

	public void fetchManualMovements(String src, Date valDate) throws IOException {
		
		Vector<Vector<Object>> tableData = new Vector<Vector<Object>>();
		
		FileInputStream fis = null;
		Workbook wb = null;
		
		try {
			fis = new FileInputStream(new File(src));
			wb = new XSSFWorkbook(fis);
			Sheet sht = wb.getSheetAt(0);
			
			Cell curr = sht.getRow(4).getCell(CellReference.convertColStringToIndex("D"));
			
			int fromAcctID = 0;
			int toAcctID = 0;
			Fund fromFund, toFund;
			double amount;
			String security;
			HoldingStatus status;
			String desc;
			String actionID;
			Optional<Integer> helixID = Optional.empty();
			Vector<Object> row = new Vector<Object>();
			
			SimpleDateFormat sdfDateChecker = new SimpleDateFormat("yyyyMMdd");
			while (!(HoldingsModel.cellEmpty(curr) && HoldingsModel.cellEmpty(curr.getRow().getCell(CellReference.convertColStringToIndex("D"))))) {
				try {
					if (sdfDateChecker.format(curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getDateCellValue()).equals(sdfDateChecker.format(valDate))) {
						
						// transaction ID used for allocations
						actionID = curr.getStringCellValue();
						
						try {
							helixID = Optional.of((int) curr.getRow().getCell(CellReference.convertColStringToIndex("E")).getNumericCellValue());
						} catch (Exception e) {
							helixID = Optional.empty();
						}
						
						boolean outgoingExists = !HoldingsModel.cellEmpty(curr.getRow().getCell(CellReference.convertColStringToIndex("F")));
						boolean incomingExists = !HoldingsModel.cellEmpty(curr.getRow().getCell(CellReference.convertColStringToIndex("G")));
						fromFund = null;
						toFund = null;
						fromAcctID = 0;
						toAcctID = 0;
						if (outgoingExists) {
							fromAcctID = (int) curr.getRow().getCell(CellReference.convertColStringToIndex("F")).getNumericCellValue();
							fromFund = model.getFundByBNYMAccountID(fromAcctID);
						}
						if (incomingExists) {
							toAcctID = (int) curr.getRow().getCell(CellReference.convertColStringToIndex("G")).getNumericCellValue();
							toFund = model.getFundByBNYMAccountID(toAcctID);
						}
						
						if (toAcctID == fromAcctID) {
							throw new NoSuchElementException(" same account, just to get there");
						}
						
						amount = curr.getRow().getCell(CellReference.convertColStringToIndex("H")).getNumericCellValue();
						security = "CASHUSD01";
						status = HoldingStatus.AVAILABLE;
						desc = actionID;
						
						// check if pairoff and if so check if matches and if so ignore so no double counting
						try {
							if (outgoingExists && !incomingExists) {
								boolean check = false;
								if (actionID.substring(0, 3).equals("PO ")) {
									// ensure instructed correct
									String cpName = actionID.substring(3, actionID.lastIndexOf(" ")).replaceAll(" ", "_"); // BNY does not allow underscores
									
									double pairoffDiff = fromFund.getPairoff(cpName) + amount;
									if (Math.abs(pairoffDiff) <= Math.abs(HoldingsModel.pairoffDiffThreshold)) { 
										System.out.println("Ignoring " + actionID + " for " + amount + " vs pairoff of " + pairoffDiff);
										check = true;
									}
																
								}
								
								// ignoring margin wire instruction as well; should have been caught by helix
								if (actionID.substring(0, 5).equals("MRGN ")) {
									System.out.println("Ignoring " + actionID + " for " + amount + ".");
									check = true;							
								}
								
								if (check) { // then ignore this entry
									if (sht.getRow(curr.getRowIndex()+1) == null) { // if whole row is null
										break;
									}
									curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("D"));
									continue;
								}
							}
							
						} catch (Exception ee) {}
						
						
						
						if (outgoingExists && incomingExists) {
													
							// ignoring swing instruction as well; should have been caught automatically
							if (!actionID.substring(0, 7).equals("HXSWING")) {								
								if (fromFund.equals(toFund)) {
									// then it's a swing
									fromFund.uploadSwing(actionID, model.getAcctNameFromBNYMID(fromAcctID), 
											 model.getAcctNameFromBNYMID(toAcctID),
										     security, 
										     status, 
										     amount, 
										     desc);
									
								} else {
									// then two transactions: one outgoing, one incoming. sensible since transactions are fund specific
									fromFund.uploadOutgoing(actionID, model.getAcctNameFromBNYMID(fromAcctID), 
										     security, 
										     status, 
										     amount,
										     desc);
									toFund.uploadIncoming(actionID, model.getAcctNameFromBNYMID(toAcctID), 
										     security, 
										     status, 
										     amount,
										     desc);
								}
							}
							
						} else if (outgoingExists) {
							fromFund.uploadOutgoing(actionID, model.getAcctNameFromBNYMID(fromAcctID), 
								     security, 
								     status, 
								     amount,
								     desc);
						} else if (incomingExists) {
							toFund.uploadIncoming(actionID, model.getAcctNameFromBNYMID(toAcctID), 
								     security, 
								     status, 
								     amount,
								     desc);
						}
						try {
							if (!actionID.substring(0, 7).equals("HXSWING")) {
								fromFund.allocateTradeBasedSwingOrWire(actionID, helixID); // here allocates it unless HXSWING
							}
						} catch (Exception e) {}
						row = new Vector<Object>();
						if (outgoingExists) { 
							row.add(fromAcctID);
						} else {
							row.add("");
						}
						if (incomingExists) {
							row.add(toAcctID);
						} else {
							row.add("");
						}
						row.add(amount);
						row.add(security);
						row.add(status);
						row.add(desc);
						tableData.add(row);
					}
				} catch (NoSuchElementException acctDNE) { // entered because acct DNE
					if (sht.getRow(curr.getRowIndex()+1) == null) { // if whole row is null
						break;
					}
					curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("D"));
					continue;
				}
				if (sht.getRow(curr.getRowIndex()+1) == null) { // if whole row is null
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("D"));
			}
			
		} catch (IOException ex) {
			throw ex;
		} finally {
			try {
				// close initializer
				fis.close();
				wb.close();
			} catch (Exception ex) {}
		}
		
		Vector<String> movementColNames = new Vector<String>();
		movementColNames.add("from acct");
		movementColNames.add("to acct");
		movementColNames.add("amount");
		movementColNames.add("security");
		movementColNames.add("holding status");
		movementColNames.add("comments");
		manualMovementsTable.setVisible(false);
		manualMovementsTable.populate(tableData, movementColNames);
		manualMovementsTable.setVisible(true);
		
		// declare as settled all net-0 transactions (eg, reallocations across series)
		for (String fn : model.getFundNames()) {
			for (Transaction t: model.getFundByName(fn).getTransactions()) {
				if (t.getActionID().length() >= 4) {
					if (t.getActionID().substring(0, 4).toUpperCase().equals("ADJ_")) {
						System.out.println("Declaring settled: " + t.getActionID());
						for (Flow fl: t.getFlows()) {
							fl.declareSettled(false);
						}
					}
					
					if (t.getActionID().substring(0, 4).toUpperCase().equals("MMF_")) {
						System.out.println("Declaring settled: " + t.getActionID());
						for (Flow fl: t.getFlows()) {
							fl.declareSettled(false);
						}
					}
				}
				if (t.getActionID().length() >= 8) {
					if (t.getActionID().substring(0, 8).toUpperCase().equals("REALLOC_")) {
						System.out.println("Declaring settled: " + t.getActionID());
						for (Flow fl: t.getFlows()) {
							fl.declareSettled(false);
						}
					}
				}
			}
		}
		
		
		// fetch non-trade allocations INVARIANT: only non-trade based (margin, pairoffs, etc.) transactions in this source 
		// will also fetch as-of trade reallocations to series (trade with earlier start date)
		try {
			fis = new FileInputStream(new File("S:\\Mandates\\Operations\\Daily Reconciliation\\Manual Allocations.xlsx"));
			wb = new XSSFWorkbook(fis);
			Sheet sht = wb.getSheetAt(0);
			
			Cell curr = sht.getRow(4).getCell(CellReference.convertColStringToIndex("B"));
			
			int fromAcctID = 0;
			int toAcctID = 0;
			Fund fromFund, toFund;
			double amount;
			String security;
			HoldingStatus status;
			String desc;
			String actionID;
			String seriesName;
			SimpleDateFormat sdfDateChecker = new SimpleDateFormat("yyyyMMdd");
			
			while (!(HoldingsModel.cellEmpty(curr) && HoldingsModel.cellEmpty(curr.getRow().getCell(CellReference.convertColStringToIndex("B"))))) {
				try {
					if (sdfDateChecker.format(curr.getRow().getCell(CellReference.convertColStringToIndex("B")).getDateCellValue()).equals(sdfDateChecker.format(valDate))) {
						
						// transaction ID used for allocations
						actionID = curr.getRow().getCell(CellReference.convertColStringToIndex("C")).getStringCellValue();
						seriesName = curr.getRow().getCell(CellReference.convertColStringToIndex("F")).getStringCellValue().toUpperCase();
						boolean outgoingExists = !HoldingsModel.cellEmpty(curr.getRow().getCell(CellReference.convertColStringToIndex("D")));
						boolean incomingExists = !HoldingsModel.cellEmpty(curr.getRow().getCell(CellReference.convertColStringToIndex("E")));
						fromFund = null;
						toFund = null;
						fromAcctID = 0;
						toAcctID = 0;
						
						if (outgoingExists) {
							fromAcctID = (int) curr.getRow().getCell(CellReference.convertColStringToIndex("D")).getNumericCellValue();
							fromFund = model.getFundByBNYMAccountID(fromAcctID);
						}
						if (incomingExists) {
							toAcctID = (int) curr.getRow().getCell(CellReference.convertColStringToIndex("E")).getNumericCellValue();
							toFund = model.getFundByBNYMAccountID(toAcctID);
						}
						
						if (toAcctID == fromAcctID) {
							throw new NoSuchElementException(" same account, just to get there");
						}
						
						amount = curr.getRow().getCell(CellReference.convertColStringToIndex("G")).getNumericCellValue();
						security = HoldingsModel.getCashRepresentation();
						status = HoldingStatus.AVAILABLE;
						desc = actionID;
						
						if (outgoingExists && !incomingExists) {
							if (fromFund.transactionExists(actionID, fromAcctID, security, status)) {
								fromFund.allocate(seriesName, actionID, -amount / fromFund.getFlowAmountToAccount(actionID, fromAcctID, security, status));
							} else {
								System.out.println("Can't find " + actionID + " to allocate to " + seriesName);
							}
						} else if (!outgoingExists && incomingExists) {
							if (toFund.transactionExists(actionID, toAcctID, security, status)) {
								toFund.allocate(seriesName, actionID, amount / toFund.getFlowAmountToAccount(actionID, toAcctID, security, status));
							} else {
								System.out.println("Can't find " + actionID + " to allocate to " + seriesName);
							}
						} else if (outgoingExists && incomingExists) {
							if (fromFund.transactionExists(actionID, fromAcctID, security, status) && toFund.transactionExists(actionID, toAcctID, security, status)) {
								if (fromFund.equals(toFund)) {
									fromFund.allocate(seriesName, actionID, -amount / fromFund.getFlowAmountToAccount(actionID, fromAcctID, security, status));
								} else {
									fromFund.allocate(seriesName, actionID, -amount / fromFund.getFlowAmountToAccount(actionID, fromAcctID, security, status));
									toFund.allocate(seriesName, actionID, amount / toFund.getFlowAmountToAccount(actionID, toAcctID, security, status));
								}
							} else {
								System.out.println("Can't find " + actionID + " to allocate to " + seriesName);
							}
						}
					}
				} catch (NoSuchElementException acctDNE) { // entered because acct DNE, but now might also enter because transaction dne
					if (sht.getRow(curr.getRowIndex()+1) == null) { // if whole row is null
						break;
					}
					curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("D"));
					continue;
				}
				if (sht.getRow(curr.getRowIndex()+1) == null) { // if whole row is null
					break;
				}
				curr = sht.getRow(curr.getRowIndex()+1).getCell(CellReference.convertColStringToIndex("D"));
			}
			
		} catch (IOException ex) {
			throw ex;
		} finally {
			try {
				// close initializer
				fis.close();
				wb.close();
			} catch (Exception ex) {}
		}
		
		table.setVisible(false);
		table.reload();
		table.setVisible(true);		
	}
	
	public void saveStateToXLSX(String templatePath, String savePath, String backupPath) throws Exception {
		this.model.saveStateToXLSX(templatePath, savePath, backupPath);
	}
	
	public void saveCFsToXLSX(String prevPath, String savePath, String backupPath, boolean projectingSoSettleAll) throws Exception {
		this.model.saveCFsToXLSX(prevPath, savePath, backupPath,projectingSoSettleAll);
		
	}

	public void bnymCashReconc(String bnyFilepath) throws IOException {
		this.model.bnymCashReconc(bnyFilepath);		
	}

	public void setTradesNotYetSettledFailing() {
		this.model.setTradesNotYetSettledFailing();
		
	}

	

}
