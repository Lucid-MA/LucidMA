# CashTracker

## Tables
The four main models are as follows.

- dbt.cash_tracker__flows_plus_allocations
    * Helix flows/Cash Blotter (dbt.cash_tracker__flows)
    * manual allocations (dbt.cash_tracker__manual_allocations)
    * failing trades (dbt.stg_lucid__failing_trades)

- dbt.stg_lucid__cash_and_security_transactions
    * BNYM (dbo.bronze_nexen_cash_and_security_transactions)

- dbt.cash_tracker__expected_flows
    * expected flows (dbt.cash_tracker__flows_plus_allocations)
    * observed flows (dbt.stg_lucid__cash_and_security_transactions)

- dbt.cash_tracker__cash_recon (replaces cash_tracker__flows_test2)
    * expected flows (dbt.cash_tracker__expected_flows)
    * observed flows (dbt.stg_lucid__cash_and_security_transactions)

## Comments
Currently dbt.cash_tracker__cash_recon is including all expected flows in balance (technically only settled/matched flows should be included). We can change this is that makes more sense.

There are more PO (cashpairoff) flows being generated, so I think we should pick a few days and debug why this is happening. The following tables build cashpairoffs.

- dbt.cash_tracker__cashpairoffs
- dbt.cash_tracker__cashpairoffs_summary