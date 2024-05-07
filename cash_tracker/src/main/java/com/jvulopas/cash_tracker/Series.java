package com.jvulopas.cash_tracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.NoSuchElementException;

public class Series implements Comparable<Series> {

	private Fund fund;
	private String name;
	
	private List<Transaction> transactions; // transactions on val date
	private HashMap<String, HashMap<String, Double>> availableHoldings;
	private HashMap<String, HashMap<String, Double>> collateralHoldings; // might be more elegant to create a Holding pojo and just have sets of that. or a non-bnym (generic) acct type
	
	private HashMap<Integer, Double> usedAllocs;
	private HashMap<String, Double> counterpartyCash;
	private HashMap<String, Double> counterpartyCashActivity;
	private HashMap<String, Double> cashPairoffs;
	
	public Series(String name, Fund fund) {
		this.name = name;
		this.fund = fund;
		counterpartyCash = new HashMap<String, Double>();
		counterpartyCashActivity = new HashMap<String, Double>();
		cashPairoffs = new HashMap<String, Double>();
		usedAllocs = new HashMap<Integer, Double>();
		availableHoldings = new HashMap<String, HashMap<String, Double>>();
		collateralHoldings = new HashMap<String, HashMap<String, Double>>();
		transactions = new ArrayList<Transaction>(); // to preserve insertion order	
		
	}

	/**
	 * @return the fund
	 */
	public Fund getFund() {
		return fund;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	
	public void uploadUsedAlloc(Integer tradeID, double portion) {
		usedAllocs.put(tradeID, portion);
	}
	
	public Optional<Double> getUsedAlloc(Integer tradeID) {
		if (usedAllocs.containsKey(tradeID)) {
			return Optional.of(usedAllocs.get(tradeID));
		}
		return Optional.empty();
	}
	
	
	public void uploadPairoff(String cp, double d, String depository) {
		double amt = 0;
		String counterparty = cp;
		if (Court.ctptiesSplittingPairoffsByDepository.contains(cp)) {
			counterparty = counterparty + "~" + depository;
		}
		if (cashPairoffs.containsKey(counterparty)) {
			amt = cashPairoffs.get(counterparty);
		}
		cashPairoffs.put(counterparty, amt + d);
		System.out.println(name + ", " + counterparty + "," + d);
	}
	
	public double getPairoff(String counterparty) {
		double outp = 0;
		if (cashPairoffs.containsKey(counterparty)) {
			outp = cashPairoffs.get(counterparty);
		}
		return outp;
	}
	
//	/**
//	 * @return the transactions
//	 */
//	public HashMap<Transaction, Double> getTransactions() {
//		return transactions;
//	}
	
	/**
	 * @return all transactions in series on valdate involving certain security with certain status
	 */
	public List<Transaction> getTransactions(String currSecurity, HoldingStatus currHoldingStatus) {
		// ordered list invariant crucial.
		List<Transaction> outp = new ArrayList<Transaction>();
		for (int i = 0; i < transactions.size(); i++) {
			for (Flow f: transactions.get(i).getFlows()) {
				if (f.getSecurity().equals(currSecurity) && f.getStatus() == currHoldingStatus) {
					outp.add(transactions.get(i));
					break; // don't forget this
				}
			}
		}
		return outp;
	}
	
	/**
	 * Upload allocated transaction to this series (separate from BNYM accounts)
	 */
	public void uploadTransaction(Transaction transaction, double portion) {
		transactions.add(transaction.makeAllocation(portion));
	}
	
	public void uploadTransactionManualAmountOverride(Transaction transaction, double manualAmount) {
		transactions.add(transaction.makeAllocationManualAmountOverride(manualAmount));
	}
	
	public double getInitialPositionInSeries(String acct, String security, HoldingStatus status) {
		switch (status) {
		case AVAILABLE :
			
			if (!availableHoldings.containsKey(acct)) {
				return 0;
			}
			
			for (String sec : availableHoldings.get(acct).keySet()) {
				if (sec.equals(security)) {
					return availableHoldings.get(acct).get(sec);
				}
			}
			
			break;
			
		case REPO_COLLATERAL:
			
			if (!collateralHoldings.containsKey(acct)) {
				return 0;
			}
			
			for (String sec : collateralHoldings.get(acct).keySet()) {
				if (sec.equals(security)) {
					return collateralHoldings.get(acct).get(sec);
				}
			}
			
			break;
		}
		
		return 0;
	}
	
//	public double getAllocation(Transaction t) throws NoSuchElementException {
//		return transactions.get(t);
//	}
//	
	/**
	 * Get movement in series on valdate. 
	 * @param acct
	 * @param security
	 * @param status
	 * @return
	 */
	public double getProjectedMovementInSeries(String acct, String security, HoldingStatus status) {
		double outp = 0;
		
		for(Transaction t : transactions) {
			outp += (t.getFlowAmountToAccount(fund.getAcctByName(acct), security, status));
		}
		
		return outp;		
	}
	
	public double getSettledMovementInSeries(String acct, String security, HoldingStatus status) {
		double outp = 0;
		
		for(Transaction t : transactions) {
			outp += (t.getSettledFlowAmountToAccount(fund.getAcctByName(acct), security, status));
		}
		
		return outp;		
	}
	
	public double getProjectedEODPositionInSeries(String acct, String security, HoldingStatus status) {
		return getInitialPositionInSeries(acct, security, status) + getProjectedMovementInSeries(acct, security, status);
	}
	
	public double getSettledEODPositionInSeries(String acct, String security, HoldingStatus status) {
		return getInitialPositionInSeries(acct, security, status) + getSettledMovementInSeries(acct, security, status);
	}
	
	public double getPairoffSizeForBNYMRepoOffset() {
		double outp = 0;
		for (String cp : this.cashPairoffs.keySet()) {
			outp += this.cashPairoffs.get(cp);
		}
		return outp;
	}
	
	/**
	 * Initialize BOD position in a security.
	 * 
	 */
	public void uploadAllocatedInitialPosition(String acctName, HoldingStatus status, String security, double amount) {
			
		switch (status) {
			case AVAILABLE :
				
				if (!availableHoldings.containsKey(acctName)) {
					availableHoldings.put(acctName, new HashMap<String, Double>());
				}
				
				if (availableHoldings.get(acctName).containsKey(security)) {
					availableHoldings.get(acctName).put(security, amount + availableHoldings.get(acctName).get(security));
				} else {
					availableHoldings.get(acctName).put(security, amount);
				}
				break;
				
			case REPO_COLLATERAL:
				
				if (!collateralHoldings.containsKey(acctName)) {
					collateralHoldings.put(acctName, new HashMap<String, Double>());
				}
				
				if (collateralHoldings.get(acctName).containsKey(security)) {
					collateralHoldings.get(acctName).put(security, amount + collateralHoldings.get(acctName).get(security));
				} else {
					collateralHoldings.get(acctName).put(security, amount);
				}
				break;
		}
	}
	
	/**
	 * Get holdings in acct at EOD valdate with given holdings status
	 * @param status
	 * @return
	 */
	public HashMap<String, Double> getEODHoldingsSet(BNYMAccount acct, HoldingStatus status) {
		HashMap<String, Double> outp = new HashMap<String, Double>();
		
		// already holding today
		Map<String, Double> bodHoldings = new HashMap<String, Double>();
		switch(status) {
		case AVAILABLE:
			bodHoldings = availableHoldings.get(acct.getName());
			break;
		case REPO_COLLATERAL:
			bodHoldings = collateralHoldings.get(acct.getName());
			break;
		}
		
		if (bodHoldings == null) bodHoldings = new HashMap<String, Double>();
		
		for (String sec: bodHoldings.keySet()) {
			outp.put(sec, this.getSettledEODPositionInSeries(acct.getName(), sec, status));
		}
		
		for (Transaction t: transactions) {
			for (Flow fl: t.getFlows()) {
				if (fl.getAccount().equals(acct) && fl.getStatus().equals(status)) {
					if (!outp.containsKey(fl.getSecurity())) {
						outp.put(fl.getSecurity(), this.getSettledEODPositionInSeries(acct.getName(), fl.getSecurity(), status));
					}
				}
			}
		}
		
		return outp;
	}
	
	public int compareTo(Series that) {
		if (that == null) return -1;
		if (this.name.equals(that.getName()) && this.fund.equals(that.getFund())) return 0;
		return -1;
	}

	public void uploadCounterpartyCashBalance(String counterparty, Double amount) {
		counterpartyCash.put(counterparty, amount);
	}
	
	public void uploadCounterpartyCashActivity(String counterparty, Double amount) {
		counterpartyCashActivity.put(counterparty, amount);
	}
	
	public double getCounterpartyCashBalance(String counterparty) {
		if (counterpartyCash.containsKey(counterparty)) {
			return counterpartyCash.get(counterparty);
		}
		
		return 0;
	}

	public HashMap<String, Double> getCounterpartyCash() {
		HashMap<String, Double> outp = new HashMap<String, Double>();
		
		for (String s: counterpartyCash.keySet()) {
			outp.put(s, counterpartyCash.get(s));
		}
		
		return outp;
	}
	
}
