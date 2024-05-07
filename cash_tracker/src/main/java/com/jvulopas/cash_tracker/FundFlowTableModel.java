package com.jvulopas.cash_tracker;

import java.util.Date;
import java.util.Optional;
import java.util.Set;

import javax.swing.table.AbstractTableModel;

public class FundFlowTableModel extends AbstractTableModel {
	
	private Fund currFund;
	
	private Optional<String> currSeries;
	private String currSecurity;
	private HoldingStatus currHoldingStatus;
	
	private Date valDate;
	private String[] cols;
	private String[] acctOrder;
	private String[] colIndicesStarts;
	
	public FundFlowTableModel(Fund f, Date valDate) {
		this.currFund = f;
		this.valDate = valDate;
		
		// default to available cash
		this.currSecurity = HoldingsModel.getCashRepresentation();
		this.currHoldingStatus = HoldingStatus.AVAILABLE;
		this.currSeries = Optional.empty(); // corresponds to fundwide
		colIndicesStarts = f.getAccountNames().toArray(new String[0]);
		
		cols = new String[(2 * colIndicesStarts.length) + 2];
		
		cols[0] = "Date";
		for (int i = 1; i < cols.length - 1; i++) {
			cols[i] = colIndicesStarts[Math.floorDiv(i-1, 2)];
			if (i % 2 == 0) {
				cols[i] += " Balance";
			} else {
				cols[i] += " Flow";
			}
		}
		cols[cols.length - 1] = "Description";
	}
	
	public FundFlowTableModel(Fund f, Date valDate, String security, HoldingStatus status, String series) {
		this.currFund = f;
		this.valDate = valDate;
		
		if (series.equals("fundwide")) {
			this.currSeries = Optional.empty();
		} else {
			this.currSeries = Optional.of(series);
		}
		
		this.currSecurity = security;
		this.currHoldingStatus = status;
		
		colIndicesStarts = f.getAccountNames().toArray(new String[0]);
		
		cols = new String[(2 * colIndicesStarts.length) + 2];
		
		cols[0] = "Date";
		for (int i = 1; i < cols.length - 1; i++) {
			cols[i] = colIndicesStarts[Math.floorDiv(i-1, 2)];
			if (i % 2 == 0) {
				cols[i] += " Balance";
			} else {
				cols[i] += " Flow";
			}
		}
		cols[cols.length - 1] = "Description";
	}
	
	public String getCurrSecurity() {
		return this.currSecurity;
	}
	

	public Date getCurrDate() {
		return this.valDate;
	}

	/**
	 * Assumes only called if fund didn't change, because only returns name of series and series names are only unique within a fund
	 * @return
	 */
	public String getCurrSeries() {
		if (this.currSeries.isEmpty()) {
			return "fundwide";
		}
		return this.currSeries.get();
	}
	
	public HoldingStatus getCurrHoldingStatus() {
		return this.currHoldingStatus;
	}
	
	public Fund getCurrFund() {
		return this.currFund; // breaks encapsulation but ok
	}
	
	public void changeSecurity(String newSecurity, HoldingStatus newStatus) {
		this.currSecurity = newSecurity;
		this.currHoldingStatus = newStatus;
	}
	
	public String getColumnName(int col) {
		return cols[col];
	}
	
	public int getRowCount() {
		if (currSeries.isEmpty()) {
			return currFund.getTransactions(currSecurity, currHoldingStatus).size() + 1; // + 1 for the beginning balance
		} else {
			return currFund.getTransactionsInSeries(currSeries.get(), currSecurity, currHoldingStatus).size() + 1;
		}
	}
	
	public int getColumnCount() {
		return cols.length;
	}
	
	// again this is not an elegant way to encapsulate series in a fund and the invariants aren't sound but necessary bc needs to catch possibility that something wasn't allocated corerctly. this isn't the allocator; it's the indicator.
	public Object getValueAt(int rowIndex, int columnIndex) {
		
		// if first column, date
		if (columnIndex == 0) {
			return valDate; // TODO instead return transaction and create transaction renderer
		}
		
		if (currSeries.isEmpty()) {
			// then fundwide, no series selected.
			
			// if last column, description
			if (columnIndex == cols.length - 1) { 
				if (rowIndex == 0) return "Beginning balance"; // TODO instead return transaction and create transaction renderer
				else {
					Transaction tran = currFund.getTransactions(currSecurity, currHoldingStatus).get(rowIndex - 1);
					return tran.getDescription();
				}
			}
			
			// else flow/balance
			BNYMAccount account = currFund.getAcctByName(colIndicesStarts[Math.floorDiv(columnIndex-1, 2)]);

			if (rowIndex == 0) {
				if (columnIndex % 2 == 0) {
					return account.getInitialPosition(currSecurity, currHoldingStatus);
				} else {
					return 0;
				}
			}
			
			// if not first row
			
			Transaction tran = currFund.getTransactions(currSecurity, currHoldingStatus).get(rowIndex - 1);
			
			Flow currFlow= tran.getFlowToAccount(account, currSecurity, currHoldingStatus);

			
			//System.out.println("JJ " + account.getName() + currFlowAmount + " - " + tran.getActionID());
			if (columnIndex % 2 != 0) {
				return currFlow;
			} else {
				// need all these params (not just flow) because can't assume that flow exists in this transaction if nothing to that account
				//return currFlowAmount + currFund.getBalance(tran, account, currSecurity, currHoldingStatus);
				return getBalance(rowIndex, columnIndex); // call recursive helper function
			}
		} else {
			// else a series. assumes exists
			// if last column, description
			if (columnIndex == cols.length - 1) { 
				if (rowIndex == 0) return "Beginning balance"; // TODO instead return transaction and create transaction renderer
				else {
					Transaction tran = currFund.getTransactionsInSeries(currSeries.get(), currSecurity, currHoldingStatus).get(rowIndex - 1);
					return tran.getDescription();
				}
			}
			
			// else flow/balance
			
			BNYMAccount account = currFund.getAcctByName(colIndicesStarts[Math.floorDiv(columnIndex-1, 2)]);
			if (rowIndex == 0) {
				if (columnIndex % 2 == 0) {
					return currFund.getInitialPositionInSeriesInAcct(currSeries.get(), account.getName(), currSecurity, currHoldingStatus);
				} else {
					return 0;
				}
			}
			
			// if not first row
			Transaction tran = currFund.getTransactionsInSeries(currSeries.get(), currSecurity, currHoldingStatus).get(rowIndex - 1);
			Flow currFlow = tran.getFlowToAccount(account, currSecurity, currHoldingStatus);
			if (columnIndex % 2 != 0) {
				return currFlow;
			} else {
				// need all these params (not just flow) because can't assume that flow exists in this transaction if nothing to that account
				//return currFlowAmount + currFund.getBalance(tran, account, currSecurity, currHoldingStatus);
				return getBalance(rowIndex, columnIndex); // call recursive helper function
			}
			
		}
		
		
		
	}
	
	/**
	 * Deciding to make the balance a view property, not model. Assumes being called from balance column
	 * @param rI
	 * @param cI
	 * @return
	 */
	private double getBalance(int rI, int cI) {
		if (rI == 0) {
			return Double.parseDouble("" + getValueAt(rI, cI));
		}
		double fl = 0;
		try {
			fl = ((Flow) getValueAt(rI, cI - 1)).getAmount();
		} catch (Exception e) {
			// TODO should never enter if flow not null
		}
		return fl + getBalance(rI - 1, cI);
	}
	

	@Override
	public boolean isCellEditable(int rowIndex, int columnIndex) {
		if (rowIndex == 0) return false; // can't edit beginning balance from sheet
		if (columnIndex == 0) return false; // can't edit date
		if (columnIndex % 2 == 0) return false; // can't edit balance column; that's auto calc'd
		return true;
	}
	
	@Override
	public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
		// follows editability invariants
		if (rowIndex == 0 || columnIndex == 0 || columnIndex % 2 == 0) {
			// TODO not enterable
		}
		
		if (columnIndex == cols.length - 1) {
			// TODO change description for that description 
		}
		try {
			if (aValue != null) {
				// TODO start here, it works. but can't make empty cell non empty (bc no flow there). double check that works and then go make write cf's
				getFlowBehindCell(rowIndex, columnIndex).setAmount(Double.parseDouble(aValue.toString()));
				//System.out.println(getFlowBehindCell(rowIndex, columnIndex).getTransaction().getFlowAmountToAccount(getFlowBehindCell(rowIndex, columnIndex).getAccount(), currSecurity, currHoldingStatus));
			}
		} catch (Exception e) {
		}
		
	}

	public Flow getFlowBehindCell(int chosenRow, int chosenCol) throws IllegalArgumentException {
		
		if (chosenCol == cols.length - 1 || chosenCol == 0 || chosenRow == 0) {
			throw new IllegalArgumentException("No flow here.");
		}
		
		if (chosenCol % 2 == 0) {
			chosenCol--;
		}
		
		BNYMAccount account = currFund.getAcctByName(colIndicesStarts[Math.floorDiv(chosenCol-1, 2)]);
		
		Transaction tran = currFund.getTransactions(currSecurity, currHoldingStatus).get(chosenRow - 1);
		
		Flow outp = tran.getFlowToAccount(account, currSecurity, currHoldingStatus);
		if (outp == null) {
			throw new IllegalArgumentException("Error getting flow");
		}
		return outp;
	}

}
