package com.jvulopas.cash_tracker;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

public class Fund implements Comparable<Fund> {
	
	private String fundID;
	private Set<BNYMAccount> accounts;
	private HashMap<Series, Double> series;
	private String sweepVehicle;
	private List<Transaction> transactions; // transactions on val date
	
	private HashMap<String, Double> counterpartyCash; // this is EOD, after all today transactions
	private HashMap<String, Double> counterpartyCashActivity;
	
	private List<Transaction> carryOverFails;
	
	private HashMap<String, Double> cashPairoffs;
	
	public Fund(String fundName, HashMap<String, Integer> accounts, HashMap<String, Double> series, String sweepVehicle) {
		this.fundID = fundName;
		this.sweepVehicle = sweepVehicle;
		this.accounts = new HashSet<BNYMAccount>();
		for (String name : accounts.keySet()) {
			this.accounts.add(new BNYMAccount(name, accounts.get(name)));
		}
		
		counterpartyCash = new HashMap<String, Double>();
		counterpartyCashActivity = new HashMap<String, Double>();
		cashPairoffs = new HashMap<String, Double>();
		
		this.series = new HashMap<Series, Double>();
		
		for (String s : series.keySet()) {
			this.series.put(
				new Series(s, this), series.get(s)
			); // the value is the default allocation ratio (this period NAV).
		}
		
		transactions = new ArrayList<Transaction>();
		carryOverFails = new ArrayList<Transaction>();
	}
	
	public Set<String> getAccountNames() {
		Set<String> outp = new HashSet<String>();
		for (BNYMAccount acct: accounts) {
			outp.add(acct.getName());
		}
		return outp;
	}
	
	public String getSweepVehicle() {
		return this.sweepVehicle;
	}
	
	/**
	 * @return all transactions in fund on valdate
	 */
	public List<Transaction> getTransactions() {
		List<Transaction> outp = new ArrayList<Transaction>();
		
		for (int i = 0; i < transactions.size(); i++) {
			outp.add(transactions.get(i));
		}
		return outp;
	}

	public boolean transactionExists(String actionID, int acctID, String security, HoldingStatus status) {
		for (Transaction t : transactions) {
			if (t.getActionID().equals(actionID) && t.concernsAccount(acctID) && t.concernsSecurityAndStatus(security, status)) {
				return true;
			}
		}
		return false;
	}

	public double getFlowAmountToAccount(String actionID, int acctID, String security, HoldingStatus status) {
		for (Transaction t : transactions) {
			if (t.getActionID().equals(actionID) && t.concernsAccount(acctID) && t.concernsSecurityAndStatus(security, status)) {
				return t.getFlowAmountToAccount(acctID, security, status);
			}
		}
		throw new NoSuchElementException("Transaction not found");
	}


	// TODO order of transactions not totally reliable in list for eg adding balance so made property of the view, which makes more sense.
//	
//	/**
//	 * @return balance in this security with this holding status up to transaction in certain account
//	 */
//	public double getBalance(Transaction tran, BNYMAccount acct, String security, HoldingStatus status) {
//		double outp = acct.getInitialPosition(security, status);
//		if (acct.getName().equals("MAIN")) { 
//			System.out.println(tran.getDescription());
//		}
//		List<Transaction> ts = getTransactions(security, status);
//		
//		for (int i = 0; i < ts.size(); i++) {
//			Transaction t = ts.get(i);
//			if (t.equals(tran)) {
//				return outp; // break before hitting this transaction
//			}
//			// list order invariant crucial
//			// works also because only one acct per transaction (via flows)
//			if (acct.getName().equals("MAIN"))  System.out.print(outp + " becomes ");
//			outp += tran.getFlowAmountToAccount(acct, security, status); // will just return 0 if that acct not present
//			if (acct.getName().equals("MAIN"))  System.out.print(outp);
//			if (acct.getName().equals("MAIN"))  System.out.println();
//		}
//		
//		return outp;
//	}
	
	/**
	 * @return all transactions in fund on valdate involving certain security with certain status
	 */
	public List<Transaction> getTransactions(String currSecurity, HoldingStatus currHoldingStatus) {
		// ordered list invariant crucial.
		List<Transaction> outp = new ArrayList<Transaction>();
		for (int i = 0; i < transactions.size(); i++) {
			for (Flow f: transactions.get(i).getFlows()) {
				if (f.getSecurity().equals(currSecurity) && f.getStatus() == currHoldingStatus) {
					outp.add(transactions.get(i));
					break; // wow not having this was causing a huge bug. remember the list invariants!
				}
			}
		}
		return outp;
	}
	
	public BNYMAccount getAcctByName(String name) throws NoSuchElementException {
		for (BNYMAccount acct: accounts) {
			if (acct.getName().equals(name)) {
				return acct;
			}
		}
		throw new NoSuchElementException("Account does not exist");
	}
	
	public boolean seriesExists(String name) {
		for (Series s: series.keySet()) {
			if (s.getName().equals(name)) {
				return true;
			}
		}
		return false;
	}
	
	private Series getSeriesByName(String name) throws NoSuchElementException {
		for (Series s: series.keySet()) {
			if (s.getName().equals(name)) {
				return s;
			}
		}
		throw new NoSuchElementException("Series " + name + " does not exist.");
	}
	
	// TODO this doesn't quite work from the security holdings perspective. but will work from cash perspective
	public void uploadPreviousFail(Transaction t) {
		this.addTransaction(t); // NOT this.transactions.add(t); breaks encapsulation
		this.carryOverFails.add(t);
		for (Flow f: t.getFlows()) {
			this.declareFail(f);
		}
	}
	
	public List<Transaction> getPreviousFails() {
		List<Transaction> outp = new ArrayList<Transaction>();
		for (Transaction t: this.carryOverFails) {
			outp.add(t);
		}	
		return outp;
	}
	
	
	public boolean isTransactionCarryOverFail(Transaction t) {
		return this.carryOverFails.contains(t);
	}
	
	
	public void allocateInitialPosition(String seriesName, String acctName, String security, double amount, HoldingStatus status) throws NoSuchElementException {
		getSeriesByName(seriesName).uploadAllocatedInitialPosition(acctName, status, security, amount);
	}
	
//  wrong because this assumes everything has been allocated. this isn't helix yet. this isn't the fund model yet. this is not the allocator. this just indics what has been done, right or wrong
//	private void allocateByNAVPortion(Transaction transaction) {
//		for (Series s : series.keySet()) {
//			s.uploadTransaction(transaction, series.get(s));
//		}
//	}
//	
//	private void allocateCustom(Transaction transaction, HashMap<String, Double> customSeriesAllocs) throws NoSuchElementException {
//		// ensure all series exist before changing any state
//		for (String s: customSeriesAllocs.keySet()) {
//			if (!seriesExists(s)) {
//				throw new NoSuchElementException("Series " + s + " does not exist,");
//			}
//		}
//		
//		for (String s : customSeriesAllocs.keySet()) {
//			getSeriesByName(s).uploadTransaction(transaction, customSeriesAllocs.get(s));
//		}
//		
//	}
//	
//	
	
	public HashSet<String> getSeriesNames() {
		HashSet<String> outp = new HashSet<String>();
		for (Series s : series.keySet()) {
			outp.add(s.getName());
		}
		return outp;
	}
	
	public void uploadInitialPosition(String acctName, String security, double amount, HoldingStatus status) throws NoSuchElementException {
		getAcctByName(acctName).uploadInitialPosition(security, amount, status);
	}
	
	private void addTransaction(Transaction transaction) {
		this.transactions.add(transaction);
		
		for (Flow f : transaction.getFlows()) {
			f.getAccount().uploadFlow(f);
		}
//		if (customSeriesAllocs == null) {
//			this.allocateByNAVPortion(transaction);
//		} else {
//			this.allocateCustom(transaction, customSeriesAllocs);
//		}
	}
	
	
	// Since this is not an allocation tool, but merely responds to alloc already been done (even if incorrect) can't enforce invariants that adds to 1, that all allocd correctly, etc. at this level
	// definitely a more elegant way to do this (encapsulate allocs win transaction, etc) but this will do at this stage.
	// 
	/**
	 * Assumes that all parts of transaction are allocated similarly (eg if one flow is x%, all flows are x%)
	 * Invariant that both moneyAlloc and parAlloc not null.
	 * Allocate a specific transaction across series.
	 * @param seriesName
	 * @param actionID
	 * @param moneyAlloc if null, will default to allocating by NAV portion
	 * @param parAlloc if null, will default to allocating by NAV portion
	 * @throws NoSuchElementException
	 */
	public void allocate(String seriesName, String actionID, Double portion) throws NoSuchElementException {
		Series s = getSeriesByName(seriesName); // will throw NSEE if needed
		
		if (portion == null) {
			System.out.println("No portion given for series " + seriesName + " allocation of " + seriesName);
			portion = series.get(s); // allocate by NAV ratio, should rarely occur
		}
		
		for (Transaction t : transactions) {
			if (t.getActionID().equals(actionID)) {
				s.uploadTransaction(t, portion); // Assumes that all parts of transaction are allocated similarly (eg if one flow is x%, all flows are x%)
			}
		}	
	}

//	// only works for available cash
//	public HashMap<Series, Double> getUsedAllocs(Integer helixID) {
//		HashMap<Series, Double> outp = new HashMap<Series, Double>();
//		
//		for (Series s: series.keySet()) {
//			for (Transaction t: s.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE)) {
//				if (t.getHelixID().isPresent()) {
//					if (t.getHelixID().get().equals(helixID)) {
//						outp.put(s, t.getPortion)
//					}
//				}
//			}
//		}
//		
//	}
	
	
	public void allocateTradeBasedSwingOrWire(String actionID, Optional<Integer> helixID) {
		if (helixID.isEmpty()) {
			return;
		}
		
		for (Series s : series.keySet()) {
			Optional<Double> portion = s.getUsedAlloc(helixID.get());
			
			if (portion.isPresent()) {
				allocate(s.getName(), actionID, portion.get());
				System.out.println("Allocated " + actionID + " to " + s.getName() + " by " + portion.get());
			}
		}		
	}
	
	
	public void allocateTradeBasedSwingOrWire(String seriesName, String actionID, Optional<Integer> helixID) {
		if (helixID.isEmpty()) {
			return;
		}
		
		for (Series s : series.keySet()) {
			if (s.getName().equals(seriesName)) {
				Optional<Double> portion = s.getUsedAlloc(helixID.get());
				
				if (portion.isPresent()) {
					allocate(s.getName(), actionID, portion.get());
					System.out.println("Allocated " + actionID + " to " + s.getName() + " by " + portion.get());
				}
			}
		}		
	}

	/**
	 * Upload master reverse repo (do not allocate)
	 * @param actionID
	 * @param start
	 * @param underlying
	 * @param counterparty
	 * @param par
	 * @param money
	 * @throws NoSuchElementException
	 */
	public void uploadReverseRepo(String actionID, boolean start, String underlying, String counterparty, double par, double money) throws NoSuchElementException {
		Transaction t1, t2;
		if (start) {
			// reverse repo opening leg, in this case money is principal amount
			t1 = new Transaction(actionID, counterparty + " reverse repo open");
			t1.addFlow(new Flow(this.getAcctByName("MAIN"), underlying, HoldingStatus.REPO_COLLATERAL, par, t1)); // debit collateral
			t2 = new Transaction(actionID, counterparty + " reverse repo open");
			t2.addFlow(new Flow(this.getAcctByName("MAIN"), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, -money, t2)); // credit cash		
		} else {
			// terminate reverse repo, in this case money is end money amount
			t1 = new Transaction(actionID, counterparty + " reverse repo term");
			t1.addFlow(new Flow(this.getAcctByName("MAIN"), underlying, HoldingStatus.REPO_COLLATERAL, -par, t1)); // credit collateral
			t2 = new Transaction(actionID, counterparty + " reverse repo term");
			t2.addFlow(new Flow(this.getAcctByName("MAIN"), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, money, t2)); // debit cash
		}
		addTransaction(t1);
		addTransaction(t2);		
//		addTransaction(t1, customSeriesAllocs);
//		addTransaction(t2, customSeriesAllocs);
		
	}
	
	/**
	 * Upload master repo (do not allocate)
	 * @param actionID
	 * @param start
	 * @param underlying
	 * @param counterparty
	 * @param par
	 * @param money
	 * @throws NoSuchElementException
	 */
	public void uploadRepo(String actionID, boolean start, String underlying, String counterparty, double par, double money) throws NoSuchElementException {
		Transaction t1, t2;
		if (start) {
			// repo opening leg, in this case money is principal amount
			t1 = new Transaction(actionID, counterparty + " repo open");
			t1.addFlow(new Flow(this.getAcctByName("MAIN"), underlying, HoldingStatus.REPO_COLLATERAL, -par, t1)); // credit collateral
			t2 = new Transaction(actionID, counterparty + " repo open");
			t2.addFlow(new Flow(this.getAcctByName("MAIN"), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, money, t2)); // debit cash
		} else {
			// terminate repo, in this case money is end money amount
			t1 = new Transaction(actionID, counterparty + " reverse repo term");
			t1.addFlow(new Flow(this.getAcctByName("MAIN"), underlying, HoldingStatus.REPO_COLLATERAL, par, t1)); // debit collateral
			t2 = new Transaction(actionID, counterparty + " reverse repo term");
			t2.addFlow(new Flow(this.getAcctByName("MAIN"), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, -money, t2)); // credit cash
		}
		addTransaction(t1);
		addTransaction(t2);
		
	}
	
	public void uploadSwing(String actionID, String fromAcct, String toAcct, String security, HoldingStatus status, double amount, String descOverride) throws NoSuchElementException {
		String desc;
		if (security.equals(HoldingsModel.getCashRepresentation())) {
			desc = "Swing cash from " + fromAcct + " to " + toAcct;
		} else {
			desc = "Swing " + security + " from " + fromAcct + " to " + toAcct;
		}
		if (descOverride != null) {
			desc = descOverride;
		}
		Transaction outp = new Transaction(actionID, desc);
		outp.addFlow(new Flow(this.getAcctByName(fromAcct), security, status, -amount, outp)); // credit
		outp.addFlow(new Flow(this.getAcctByName(toAcct),security, status, amount, outp)); // debit
		addTransaction(outp);
	}

	public void uploadOutgoing(String actionID, String fromAcct, String security, HoldingStatus status,
			double amount, String descOverride) throws NoSuchElementException {
		String desc;
		if (security.equals(HoldingsModel.getCashRepresentation())) {
			desc = "Transfer cash from " + fromAcct ;
		} else {
			desc = "Transfer " + security + " from " + fromAcct;
		}
		if (descOverride != null) {
			desc = descOverride;
		}
		Transaction outp = new Transaction(actionID, desc);
		outp.addFlow(new Flow(this.getAcctByName(fromAcct), security, status, -amount, outp)); // credit
		addTransaction(outp);		
	}
	
	public void uploadIncoming(String actionID, String acct, String security, HoldingStatus status,
			double amount, String descOverride) throws NoSuchElementException {
		String desc;
		if (security.equals(HoldingsModel.getCashRepresentation())) {
			desc = "Receive cash into " + acct ;
		} else {
			desc = "Receive " + security + " into " + acct;
		}
		if (descOverride != null) {
			desc = descOverride;
		}
		Transaction outp = new Transaction(actionID, desc);
		outp.addFlow(new Flow(this.getAcctByName(acct), security, status, amount, outp)); // debit
		addTransaction(outp);		
	}
	
	
	/*
	 * MARGIN PARADIGM:
	 * INVARIANT: we only close and rebook net if balance switching signs
	 * 
	 * OUTGOING MARGIN
	 * 	If cash held > call amount, then swing CALL amount from margin -> main and wire CALL amount out of main
	 * 			(reversefree close & repofree open)
	 * 	If cash held < call amount then swing HELD amount from margin -> main and wire CALL amount out of main
	 *			(reversefree close & repofree open) (if held = 0 then just repofree open)
	 * INCOMING MARGIN
	 * 	If cash posted > call amount then receive CALL amount -> main
	 * 			(repofree close & reversefree open)
	 *  If cash posted < call amount then receive CALL amount -> main and swing (CALL - POSTED) from main -> margin
	 *  		(repofree close & reversefree open) (if posted = 0 then just reversefree open)
	 *  
	 */
	
	public static boolean shouldSwing(boolean reverseFree, boolean open, double cpCashBOD, double cpCashEOD) {
		
		if (reverseFree && open) {
			// swing main to margin iff 0 to +, - to +, or + to +
			return cpCashEOD > 0;
		}
		
		if (reverseFree && !open) {
			// swing margin to main iff + to 0, + to -, or + to +
			return cpCashBOD > 0;
		}
		
		if (!reverseFree && open) {
			// swing margin to main iff + to +
			return cpCashBOD > 0 && cpCashEOD > 0;
		}
		
		if (!reverseFree && !open) {
			// swing main to margin iff + to 0, + to -, or + to +
			return cpCashBOD > 0;
		}
		
		return false; // unreachable
		
		
	}
	
	
	public void uploadIncomingMargin(String actionID, String security, boolean start, String counterparty, double amount) throws NoSuchElementException {
		Transaction outp;
		if (security.substring(0,3).equals("PNI")) {
			return; // ignore PNI 
		}
		double marginBalBOD = 0;
		double marginBalEOD = 0;
		if (counterpartyCash.keySet().contains(counterparty)) {
			marginBalEOD = counterpartyCash.get(counterparty);
		}
		if (counterpartyCashActivity.keySet().contains(counterparty)) {
			marginBalBOD = marginBalEOD - counterpartyCashActivity.get(counterparty);
		} else {
			marginBalBOD = marginBalEOD;
		}
		
		String swingID = "HXSWING" + actionID.substring(0, actionID.indexOf(" "));
		if (start) {
			// margin receipt
			// made decision to settle directly into/out of margin account (not main) for reconc purposes, thus assuming that swing is correctly intructed as opposed to depending on it
			// handling that by assuming that swing is instructed here instead of waiting for it to be instructed later, then ignoring it from instructed blotter (just like with outgoing margin wires)
			outp = new Transaction(actionID, "Receive " + counterparty + " margin"); 
			outp.addFlow(new Flow(this.getAcctByName("MAIN"), security, HoldingStatus.AVAILABLE, amount, outp)); // technically the margin account treats it as available cash
			
			if (shouldSwing(true, true, marginBalBOD, marginBalEOD)) {
				uploadSwing(swingID, "MAIN", "MARGIN", security, HoldingStatus.AVAILABLE, amount, swingID); // so project here that swing happens, not later because saw it in cash blotter.
			}
			
		} else {
			// return received margin
			outp = new Transaction(actionID, "Return " + counterparty + " margin");
			outp.addFlow(new Flow(this.getAcctByName("MAIN"), security, HoldingStatus.AVAILABLE, -amount, outp)); // technically the margin account treats it as available cash
			if (shouldSwing(true, false, marginBalBOD, marginBalEOD)) {
				uploadSwing(swingID+"CLS", "MARGIN", "MAIN", security, HoldingStatus.AVAILABLE, amount, swingID+"CLS");
			}
		}
		
		addTransaction(outp);
	}
	
	/*
	 * Conversely to the above, nothing directly outgoing should have to do with the margin account (if returning held margin, handled in the above function as is close of RVF. 
	 */
	
	public void uploadOutgoingMargin(String actionID, String security, boolean start, String counterparty, double amount) throws NoSuchElementException {
		Transaction outp;
		if (security.substring(0,3).equals("PNI")) {
			return; // ignore PNI 
		}
		double marginBalBOD = 0;
		double marginBalEOD = 0;
		if (counterpartyCash.keySet().contains(counterparty)) {
			marginBalEOD = counterpartyCash.get(counterparty);
		}
		if (counterpartyCashActivity.keySet().contains(counterparty)) {
			marginBalBOD = marginBalEOD - counterpartyCashActivity.get(counterparty);
		} else {
			marginBalBOD = marginBalEOD;
		}
		String swingID = "HXSWING" + actionID.substring(0, actionID.indexOf(" "));
		
		if (start) {
			// margin payment
			outp = new Transaction(actionID, "Pay " + counterparty + " margin"); // this ought to stay in main 
			outp.addFlow(new Flow(this.getAcctByName("MAIN"), security, HoldingStatus.AVAILABLE, -amount, outp));
			if (shouldSwing(false, true, marginBalBOD, marginBalEOD)) {
				uploadSwing(swingID, "MARGIN", "MAIN", security, HoldingStatus.AVAILABLE, amount, swingID);
			}
		} else {
			// receive returning margin
			outp = new Transaction(actionID, "Receive returned " + counterparty + " margin");
			outp.addFlow(new Flow(this.getAcctByName("MAIN"), security, HoldingStatus.AVAILABLE, amount, outp));
			if (shouldSwing(false, false, marginBalBOD, marginBalEOD)) {
				uploadSwing(swingID+"CLS", "MARGIN", "MAIN", security, HoldingStatus.AVAILABLE, -amount, swingID+"CLS");
			}
		}
		
		addTransaction(outp);
	}
	
	// determine whether all have been allocated
	public HashSet<Transaction> notAllocated() {
		HashSet<Transaction> outp = new HashSet<Transaction>();
		if (series.keySet().size() <= 1) {
			return outp;
		}
		double checkSum = 0;
		for (Transaction t: transactions) {
			for (Flow fl: t.getFlows()) {
				checkSum = fl.getAmount();
				for (Flow ffl : getAllocations(fl)) {
					checkSum -= ffl.getAmount();
					if (t.getActionID().equals("PMSWING00301")) {
						System.out.println(ffl.getAmount());
					}
				}
				if (Math.abs(checkSum) >= 0.03) {
					outp.add(t);
					System.out.println("NOT ALLOCATED: " + t.getActionID());
				}
			}
		}
		return outp;
	}

	public List<Transaction> getTransactionsInSeries(String seriesName, String currSecurity,
			HoldingStatus currHoldingStatus) {
		return this.getSeriesByName(seriesName).getTransactions(currSecurity, currHoldingStatus);
 	}

	public double getInitialPositionInSeriesInAcct(String seriesName, String acctName, String security,
			HoldingStatus status) {
		return this.getSeriesByName(seriesName).getInitialPositionInSeries(acctName, security, status);
	}

	public double getEODPositionInSeriesInAcct(String seriesName, String acctName, String security,
			HoldingStatus status) {
		return this.getSeriesByName(seriesName).getSettledEODPositionInSeries(acctName, security, status);
	}
	
	public double getProjectedEODPositionInSeriesInAcct(String seriesName, String acctName, String security,
			HoldingStatus status) {
		return this.getSeriesByName(seriesName).getProjectedEODPositionInSeries(acctName, security, status);
	}

	public HashMap<String, Double> getEODHoldingsSetInSeriesInAcct(String seriesName, String acctName,
			HoldingStatus status) {
		return this.getSeriesByName(seriesName).getEODHoldingsSet(this.getAcctByName(acctName), status);
	}

	/**
	 * Get allocations of a flow
	 * @param f
	 * @return
	 */
	public Set<Flow> getAllocations(Flow f) {
		Set<Flow> outp = new HashSet<Flow>();
		for (Series s: series.keySet()) {
			for (Transaction t: s.getTransactions(f.getSecurity(), f.getStatus())) {
				if (t.isAllocationOf(f.getTransaction())) { // if same transaction
					for (Flow fl : t.getFlows()) {
						if (fl.getAccount().equals(f.getAccount())) {
							outp.add(fl);
						}
					}
				}
			}
		}
		return outp;
	}
	
	// so can allocate trade based swings and wires
	public void uploadUsedAlloc(String seriesName, Integer tradeID, double portion) {
		for (Series s : series.keySet()) {
			if (s.getName().equals(seriesName)) {
				s.uploadUsedAlloc(tradeID, portion);
			}
		}
		
	}

	/**
	 * Set all trades that have not yet settled as failing.
	 */
	public void setTradesNotYetSettledFailing() {
		for (Transaction t : this.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE)) {
			for (Flow f: t.getFlows()) {
				if (f.hasSettled().isEmpty()) {
					declareFail(f);
				}
			}
		}
	}

	/**
	 * Not perfectly encapsulated because still public declare failing method in the flow class.
	 * But allocation wise makes more sense to do it from here.
	 * @param f
	 */
	public void declareFail(Flow f) {
		f.declareFailing();
		for (Flow fl : getAllocations(f)) {
			fl.declareFailing();
		}
		
	}
	
	public void declareSettle(Flow f, boolean afterSweep) {
		f.declareSettled(afterSweep);
		for (Flow fl : getAllocations(f)) {
			fl.declareSettled(afterSweep);
		}
		
		
	}
	
	/**
	 * Sweep cash on ValDate into sweep vehicle. This is after the EOD Positions are calculated (as it accounts for failing trades). 
	 * 
	 * 
	 */
	public void MMSweep() {
		
		this.setTradesNotYetSettledFailing();
	
		
		Transaction t1 = null;
		Transaction t2 = null;

		
		for (BNYMAccount acct : accounts) {
			
			//settled EOD position 
			double sweepAmount = -acct.getEODPosition(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
			//System.out.println("cash amount is " + (-sweepAmount) + " acct " + acct.getAcctNumber());
			// remove all trades settled late
			for (Transaction t : this.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE)) {
				for (Flow f: t.getFlows()) {
					if (f.getAccount().equals(acct) && f.hasSettled().isPresent()) {
						if (!(f.hasSettled().get().booleanValue())) {
							System.out.println("Detected failing " + f.getTransaction().getDescription());
							if (f.getAmount() < 0 && f.getTransaction().getHelixID().isPresent() && !(f.getTransaction().getDescription().contains("margin")) && !(f.getTransaction().getDescription().contains("HXSWING"))) {
								// if failing outflow that's pending (ie, BNY aware of it), BNY will not sweep the cash
								System.out.println("Leaving unswept " + f.getTransaction().getDescription());
								sweepAmount -= f.getAmount();
							}
							
						} else if (f.settledAfterSweep()) {
							// settled after sweep
							System.out.println("Detected settled after sweep " + f.getTransaction().getDescription());
							sweepAmount += f.getAmount();
						} // else settled normally
					}
				}
			}
			
//			if (Math.abs(sweepAmount) <= 0.01) {
//				continue;
//			}
			
			t1 = new Transaction("MMSweepCASH" + acct.getName(), "MM Sweep");
			t2 = new Transaction("MMSweepVEH" + acct.getName(), "MM Sweep");
			t1.addFlow(new Flow(acct, HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, sweepAmount, t1));
			t2.addFlow(new Flow(acct, sweepVehicle, HoldingStatus.AVAILABLE, -sweepAmount, t2));
			addTransaction(t1);
			addTransaction(t2);
			
			for (Flow fll : t1.getFlows()) {
				fll.declareSettled(false);
			}
			
			for (Flow fll : t2.getFlows()) {
				fll.declareSettled(false);
			}
			
			// allocate 
			for (Series s : series.keySet()) {
				double seriesAmount = -s.getSettledEODPositionInSeries(acct.getName(), HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
				// remove all failing trades 
				for (Transaction t : this.getTransactionsInSeries(s.getName(),HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE)) {
					for (Flow f: t.getFlows()) {
						if (f.getAccount().equals(acct) && f.hasSettled().isPresent()) {
							if (!(f.hasSettled().get().booleanValue())) {
								if (f.getAmount() < 0 && f.getTransaction().getHelixID().isPresent() && !(f.getTransaction().getDescription().contains("margin")) && !(f.getTransaction().getDescription().contains("HXSWING"))) {
									// if failing outflow that's pending (ie, BNY aware of it), BNY will not sweep the cash
									System.out.println("Leaving unswept " + f.getTransaction().getDescription());
									seriesAmount -= f.getAmount();
								}
							} else if (f.settledAfterSweep()) {
								// settled after sweep
								System.out.println("Detected settled after sweep " + f.getTransaction().getDescription());
								seriesAmount += f.getAmount();
							} // else settled normally
						}
					}
				}		
				
				if (sweepAmount == 0) {
					s.uploadTransactionManualAmountOverride(t1, seriesAmount);
					s.uploadTransactionManualAmountOverride(t2, -seriesAmount);
				} else {
					s.uploadTransaction(t1, seriesAmount/sweepAmount);
					s.uploadTransaction(t2, seriesAmount/sweepAmount);
				}

			}
					
		}
	}
	
	public void uploadCounterpartyCashBalance(String counterparty, Double amount) {
		counterpartyCash.put(counterparty, amount);
	}
	
	public void uploadCounterpartyCashActivity(String counterparty, Double amount) {
		counterpartyCashActivity.put(counterparty, amount);
	}
	
	public void allocateCounterpartyCashBalance(String seriesName, String counterparty, Double amount) {
		this.getSeriesByName(seriesName).uploadCounterpartyCashBalance(counterparty, amount);
	}
	
	public void allocateCounterpartyCashActivity(String seriesName, String counterparty, Double amount) {
		this.getSeriesByName(seriesName).uploadCounterpartyCashActivity(counterparty, amount);
	}
	
	public double getCounterpartyCashBalance(String counterparty) {
		if (counterpartyCash.containsKey(counterparty)) {
			return counterpartyCash.get(counterparty);
		}
		return 0;
	}
	
	public double getCounterpartyCashBalanceInSeries(String seriesName, String counterparty) throws NoSuchElementException {
		return this.getSeriesByName(seriesName).getCounterpartyCashBalance(counterparty);
	}
	
	public HashMap<String, Double> getCounterpartyCash() {
		HashMap<String, Double> outp = new HashMap<String, Double>();
		
		for (String s: counterpartyCash.keySet()) {
			outp.put(s, counterpartyCash.get(s));
		}
		
		return outp;
	}
	
	public HashMap<String, Double> getCounterpartyCashInSeries(String seriesName) throws NoSuchElementException {
		HashMap<String, Double> outp = new HashMap<String, Double>();
		HashMap<String, Double> frm = this.getSeriesByName(seriesName).getCounterpartyCash();
		for (String s: frm.keySet()) {
			outp.put(s, frm.get(s));
		}
		
		return outp;
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
		System.out.println(counterparty + "," + d);
	}
	
	public void uploadPairoffIntoSeries(String seriesName, String counterparty, double d, String depository) {
		getSeriesByName(seriesName).uploadPairoff(counterparty, d, depository);
	}
	
	public double getPairoff(String counterparty) {
		double outp = 0;
		if (cashPairoffs.containsKey(counterparty)) {
			outp = cashPairoffs.get(counterparty);
		}
		return outp;
	}
	
	public double getPairoffSizeForBNYMRepoOffset() {
		double outp = 0;
		for (String cp : this.cashPairoffs.keySet()) {
			outp += this.cashPairoffs.get(cp);
		}
		return outp;
	}
	
	public void uploadPairoffsIntoTransactions() {
		for (String cp: cashPairoffs.keySet()) {
			if (Math.abs(cashPairoffs.get(cp)) > 0) {
				uploadIncoming("PO " + cp, "MAIN", HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, cashPairoffs.get(cp), "PO " + cp);
				for (Series s: this.series.keySet()) {
					if (Math.abs(s.getPairoff(cp)) > 0) {
						double tmp1229 = 0;
						allocate(s.getName(), "PO " + cp, (tmp1229+ s.getPairoff(cp))/cashPairoffs.get(cp));
					}
					
				}
			}
		}
		
//		double netPO = getPairoffSizeForBNYMRepoOffset();
//		uploadIncoming("BNYM_REPO_OFFSET", "MAIN", HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, -netPO,"BNYM_REPO_OFFSET");
//		for (Series s: this.series.keySet()) {
//			if (Math.abs(s.getPairoffSizeForBNYMRepoOffset()) > 0) {
//				allocate(s.getName(), "BNYM_REPO_OFFSET", s.getPairoffSizeForBNYMRepoOffset()/getPairoffSizeForBNYMRepoOffset());
//			}
//			
//		}
		
		// cashPairoffs.clear();
	}

//	/**
//	 * sweep cash on ValDate into sweep vehicle
//	 */
//	public void MMSweep() {
//		Transaction t1 = null;
//		Transaction t2 = null;
//		
//		t1 = new Transaction("MMSweepCASH", "MM Sweep"); // can only MM Sweep once
//		t2 = new Transaction("MMSweepVEH", "MM Sweep");
//		
//		for (BNYMAccount acct : accounts) {
//			double sweepAmount = acct.getEODPosition(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE);
//			t1.addFlow(new Flow(acct, HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE, -sweepAmount, t1));
//			t2.addFlow(new Flow(acct, sweepVehicle, HoldingStatus.AVAILABLE, sweepAmount, t2));
//		}
//		addTransaction(t1);
//		addTransaction(t2);
//		
//		// allocate
//		for (Series s : series.keySet()) {
//			s.uploadTransaction(t1, series.get(s));
//			s.uploadTransaction(t2, series.get(s));
//		}
//		
//		//System.out.println(this.fundID + " " + this.getTransactions(HoldingsModel.getCashRepresentation(), HoldingStatus.AVAILABLE).size());
//	}
//	
	
	public String getFundID() {
		return fundID;
	}
	
	@Override
	public boolean equals(Object that) {
		if (that == null) {
			return false;
		}
		
		if (!(that instanceof Fund)) {
			return false;
		}
		
		return this.fundID == ((Fund) that).getFundID();
		
	}
	
	public int compareTo(Fund that) {
		if (this == that) {
			return 0;
		}
		return -1;
	}
	
	@Override
	public String toString() {
		return this.fundID;
	}



	



}
