package com.jvulopas.cash_tracker;

import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * A transaction is the simultaneous movement of securities to/from accounts.
 * Each transaction is one "line" on the flow statement sheet, ie, each custodial account may be represented once and only once.
 * @author jvulopas
 *
 */
public class Transaction {
	
	private String description;
	private Set<Flow> flows;
	private String actionID;
	private Optional<Date> settleDate; // only relevant if different from valDate, else empty
	private Optional<Transaction> allocationOf;
	
	public Transaction(String actionID, String description) {
		this.description = description;
		this.flows = new HashSet<Flow>();
		this.actionID = actionID;
		this.settleDate = Optional.empty();
		this.allocationOf = Optional.empty();
	}
	
	
	public Transaction(String actionID, String description, Date settleDate) {
		this.description = description;
		this.flows = new HashSet<Flow>();
		this.actionID = actionID;
		this.settleDate = Optional.of(settleDate);
	}
	
	public Optional<Date> getDate() {
		return this.settleDate;
	}
	
	public String getActionID() {
		return actionID;
	}
	
	public void setAllocationOf(Transaction t) {
		this.allocationOf = Optional.of(t);
	}
	
	public Optional<Integer> getHelixID() {
		try {
			if (this.description.substring(0, 7).equals("HXSWING")) {
				try {
					return Optional.of(Integer.parseInt(this.description.substring(7, this.description.length())));
				} catch (Exception e) {
					return Optional.of(Integer.parseInt(this.description.substring(7, this.description.indexOf("C"))));
				}
				
			} else {
				try {
					return Optional.of(Integer.parseInt(this.actionID.substring(0, this.actionID.indexOf(" "))));
				} catch (Exception e) {
					return Optional.of(Integer.parseInt(this.actionID.substring(0, this.actionID.indexOf("C"))));
				}
			}
		} catch (Exception e) {
		    return Optional.empty();
		}
	}
	
	public boolean concernsAccount(BNYMAccount acct) {
		for (Flow f : flows) {
			if (f.getAccount().equals(acct)) {
				return true;
			}
		}
		return false;
	}
	
	public boolean concernsAccount(int acctID) {
		for (Flow f : flows) {
			if (f.getAccount().getAcctNumber() == acctID) {
				return true;
			}
		}
		return false;
	}
	
	public boolean concernsSecurityAndStatus(String security, HoldingStatus status) {
		for (Flow f : flows) {
			if (f.getStatus() == status && f.getSecurity().equals(security)) {
				return true;
			}
		}
		return false;
	}
	
	public String getDescription() {
		return this.description;
	}
	
	public void addFlow(Flow flow) throws IllegalArgumentException {
		if (concernsAccount(flow.getAccount())) {
			
			
			throw new IllegalArgumentException("Transaction alreaday considers account " + flow.getAccount());
		}
		flows.add(flow);
	}
	
	public Set<Flow> getFlows() {
		Set<Flow> outp = new HashSet<Flow>();
		for (Flow f: flows) {
			outp.add(f);
		}
		return outp;
	}
	

	public boolean isAllocationOf(Transaction t) {
		boolean outp = false;
		if (this.allocationOf.isPresent()) {
			outp = this.allocationOf.get() == t;
		}
		return outp;
	}
	/**
	 * Make "allocated" version of a transaction. Encapsulated within a series.
	 * Linked to master through actionID.
	 * @param portion
	 * @return
	 */
	public Transaction makeAllocation(double portion) {
		Transaction outp;
		if (settleDate.isPresent()) {
			outp = new Transaction(actionID, description, settleDate.get());
		} else {
			outp = new Transaction(actionID, description);
		}
		for (Flow f : flows) {
			if (f.getAccount().getName().toUpperCase().equals("EXPENSE")) {
				outp.addFlow(f.makeAllocation(0)); // ignore expense account allocations at series level
			} else {
				outp.addFlow(f.makeAllocation(portion));
			}
		}
		outp.setAllocationOf(this); // crucial step; other callers use referential equality
		return outp;
	}
	
	// repeats code but OK
	public Transaction makeAllocationManualAmountOverride(double manualAmount) {
		Transaction outp = new Transaction(actionID, description);
		for (Flow f : flows) {
			outp.addFlow(f.makeAllocationManualAmountOverride(manualAmount));
		}
		outp.setAllocationOf(this); // crucial step; other callers use referential equality
		return outp;
	}
	
	

	// reusing code but okay
	public double getFlowAmountToAccount(int acctID, String security, HoldingStatus holdingStatus) {
		for (Flow f: flows) {
			if (f.getAccount().getAcctNumber() == acctID && f.getSecurity().equals(security) && f.getStatus().equals(holdingStatus)) {
				return f.getAmount();
			}
		}
		return 0;
	}
	
	public double getFlowAmountToAccount(BNYMAccount account, String security, HoldingStatus holdingStatus) {
		for (Flow f: flows) {
			if (f.getAccount().equals(account) && f.getSecurity().equals(security) && f.getStatus().equals(holdingStatus)) {
				return f.getAmount();
			}
		}
		return 0;
	}
	
	public double getSettledFlowAmountToAccount(BNYMAccount account, String security, HoldingStatus holdingStatus) {
		for (Flow f: flows) {
			if (f.getAccount().equals(account) && f.getSecurity().equals(security) && f.getStatus().equals(holdingStatus)) {
				if (f.hasSettled().isPresent()) {
					if (f.hasSettled().get()) {
						return f.getAmount();
					}
				}
				
			}
		}
		return 0;
	}

	public Flow getFlowToAccount(BNYMAccount account, String security, HoldingStatus holdingStatus) {
		for (Flow f: flows) {
			if (f.getAccount().equals(account) && f.getSecurity().equals(security) && f.getStatus().equals(holdingStatus)) {
				return f;
			}
		}
		return null;
	}



	

}
