package com.jvulopas.cash_tracker;

public enum HoldingStatus {
	AVAILABLE, REPO_COLLATERAL; // todo if add go into the funds, accts, series, and add another map for avail vs collat holdings. better to make map from enum to map of holdings (either set of holdings object or Map<String, Double>)
}