package com.jvulopas.cash_tracker;

import java.util.Optional;

/**
 * A flow is an increase or decrease of quantity of some security in some account.
 * It's one component of a transaction, which consists of multiple simultaneous flows.
 * @author jvulopas
 *
 */
public class Flow {
	private BNYMAccount account;
	private String security;
	private HoldingStatus status;
	private double amount;
	private Transaction transaction; // transaction housing this flow
	
	private Optional<Boolean> settled;
	private boolean settledAfterSweep;

	public Flow(BNYMAccount account, String security, HoldingStatus status, double amount, Transaction transaction) {
		this.account = account;
		this.security = security;
		this.status = status;
		this.amount = amount;
		this.transaction = transaction;
		this.settled = Optional.empty();
		this.settledAfterSweep = false;
	}
	
	
	public Optional<Boolean> hasSettled() {
		if (settled.isEmpty()) return Optional.empty();
		return Optional.of(settled.get());
	}
	
	/**
	 * @return the account
	 */
	public BNYMAccount getAccount() {
		return account;
	}

	/**
	 * @return the security
	 */
	public String getSecurity() {
		return security;
	}


	/**
	 * @return the status
	 */
	public HoldingStatus getStatus() {
		return status;
	}


	/**
	 * @return the amount
	 */
	public double getAmount() {
		return amount;
	}
	

	public void setAmount(double aValue) {
		this.amount = aValue;
		
	}
	

	/**
	 * @return the transaction
	 */
	public Transaction getTransaction() {
		return transaction;
	}

	/**
	 * Allocate this flow to a series (linked through actionID at transaction level)
	 * The crucial rounding moment takes place here.
	 * @param portion
	 * @return
	 */
	public Flow makeAllocation(double portion) {
		Flow outp = new Flow(account, security, status, Math.round(portion * amount * 100.0)/100.0, transaction);
		
		if (settled.isPresent()) {
			if (settled.get().booleanValue()) {
				outp.declareSettled(settledAfterSweep);
			} else {
				outp.declareFailing();
			}
		}
		
		return outp;
	}
	
	public Flow makeAllocationManualAmountOverride(double manualAmount) {
		Flow outp = new Flow(account, security, status, Math.round(manualAmount * 100.0)/100.0, transaction);
		
		if (settled.isPresent()) {
			if (settled.get().booleanValue()) {
				outp.declareSettled(settledAfterSweep);
			} else {
				outp.declareFailing();
			}
		}
		
		return outp;
	}

	public void declareFailing() {
		settled = Optional.of(false);
	}
	
	public void declareSettled(boolean afterSweep) {
		settled = Optional.of(true);
		settledAfterSweep = afterSweep;
	}


	public boolean settledAfterSweep() {
		return settledAfterSweep;
	}


}
