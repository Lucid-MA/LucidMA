package com.jvulopas.cash_tracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 
 * Model BNYM account with initial balance in cash and securities
 * @author jvulopas
 *
 */

public class BNYMAccount implements Comparable<BNYMAccount> {
	
	private String name;
	private int acctNumber;
	
	private Map<String, Double> availableHoldings; // initial (BOD) positions in available securities/cash
	private Map<String, Double> collateralHoldings; // initial (BOD) positions in securities/cash held as repo collateral
	
	// just flows today
	private List<Flow> flows;
	
	/**
	 * Initialize BNYM account with 0 cash balance and no securities positions.
	 */
	public BNYMAccount(String name, int acctNumber) {
		this.name = name;
		this.acctNumber = acctNumber;
		
		this.flows = new ArrayList<Flow>();
		this.availableHoldings = new HashMap<String, Double>(); // BOD holdings
		this.collateralHoldings = new HashMap<String, Double>(); // BOD holdings
	}
	
	
	public int getAcctNumber() {
		return this.acctNumber;
	}
	
	public Set<String> getInitialAvailableHoldingsSet() {
		Set<String> outp = new HashSet<String>();
		
		for (String s : availableHoldings.keySet()) {
			outp.add(s);
		}
		return outp;
	}
	
	public Set<String> getInitialCollateralHoldingsSet() {
		Set<String> outp = new HashSet<String>();
		
		for (String s : collateralHoldings.keySet()) {
			outp.add(s);
		}
		return outp;
	}
	
	
	/**
	 * Initialize BOD position in a security.
	 * 
	 */
	public void uploadInitialPosition(String security, double amount, HoldingStatus status) {
		switch (status) {
			case AVAILABLE :
				if (availableHoldings.containsKey(security)) {
					availableHoldings.put(security, amount + availableHoldings.get(security));
				} else {
					availableHoldings.put(security, amount);
				}
				break;
			case REPO_COLLATERAL:
				if (collateralHoldings.containsKey(security)) {
					collateralHoldings.put(security, amount + collateralHoldings.get(security));
				} else {
					collateralHoldings.put(security, amount);
				}
				break;
		}
	}
	
	/**
	 * Upload flow (increase/reduction in security/cash position on valdate, housed in some transaction
	 */
	public void uploadFlow(Flow flow) {
		this.flows.add(flow);
	}
	
	public double getInitialPosition(String security, HoldingStatus status) {
		switch (status) {
			case AVAILABLE :
				if (availableHoldings.containsKey(security)) {
					return availableHoldings.get(security);
				} else {
					return 0;
				}
			case REPO_COLLATERAL:
				if (collateralHoldings.containsKey(security)) {
					return collateralHoldings.get(security);
				} else {
					return 0;
				}
			default:
				return 0;
		}
	}
	
	public double getMovementOnValDate(String security, HoldingStatus status) {
		double outp = 0;
		for (Flow f: flows) {
			if (f.getSecurity().equals(security) && f.getStatus().equals(status)) {
				// settlement piece handled here**at fund level** at series level it's separated out
				if (f.hasSettled().isPresent()) {
					if (f.hasSettled().get()) {
						System.out.println(f.getTransaction().getDescription() + " has settled");
						outp += f.getAmount();
					}
				}
			}
		}
		
		return outp;
	}
	
	public double getProjectedMovementOnValDate(String security, HoldingStatus status) {
		double outp = 0;
		for (Flow f: flows) {
			if (f.getSecurity().equals(security) && f.getStatus().equals(status)) {
				outp += f.getAmount();
			}
		}
		return outp;
	}
	
	/**
	 * EOD position in a security (before the sweep)
	 * 
	 * @param security
	 * @param status
	 * @return
	 */
	public double getEODPosition(String security, HoldingStatus status) {
		return getInitialPosition(security, status) + getMovementOnValDate(security, status);
	}
	
	
	public double getProjectedEODPosition(String security, HoldingStatus status) {
		return getInitialPosition(security, status) + getProjectedMovementOnValDate(security, status);
	}

	public boolean equals(Object that) {
		if (that == null) {
			return false;
		}
		
		if (!(that instanceof BNYMAccount)) {
			return false;
		}
		
		return this.acctNumber == ((BNYMAccount) that).getAcctNumber();
	}
	
	public int compareTo(BNYMAccount that) {
		return this.acctNumber - that.getAcctNumber();
	}


	public String getName() {
		return this.name;
	}


	/**
	 * Get holdings in acct at EOD valdate with given holdings status
	 * @param status
	 * @return
	 */
	public HashMap<String, Double> getEODHoldingsSet(HoldingStatus status) {
		HashMap<String, Double> outp = new HashMap<String, Double>();
		
		// already holding today
		Map<String, Double> bodHoldings = new HashMap<String, Double>();
		switch(status) {
		case AVAILABLE:
			bodHoldings = availableHoldings;
			break;
		case REPO_COLLATERAL:
			bodHoldings = collateralHoldings;
			break;
		}
		
		for (String sec: bodHoldings.keySet()) {
			outp.put(sec, getEODPosition(sec, status));
		}
		
		for (Flow fl: flows) {
			if (fl.getStatus().equals(status)) {
				if (!outp.containsKey(fl.getSecurity())) { // if not already there, include it
					outp.put(fl.getSecurity(), this.getEODPosition(fl.getSecurity(), status));
				}
			}
		}
		
		return outp;
	}
}


