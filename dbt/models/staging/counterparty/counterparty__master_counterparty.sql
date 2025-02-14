{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "schema": 'dbo',
    })
}}

WITH
counterparties AS (
    SELECT
        counterparty_helix_name,
        counterparty_legal_name,
        manager,
        guarantor,
        place_of_business,
        group_type,
        entity_type,
        stock_ticker,
        short_description,
        public_or_private,
        risk_note,
        transparency_data,
        transparency_method,
        transparency_location,
        transparency_contact,
        transparency_frequency,
        auto_or_manual
    FROM {{ source('sql2', 'counterparties') }}
),

counterparty_ic_review AS (
    SELECT
        counterparty_legal_name,
        last_ic_review_date,
        initial_ic_review_date,
        prime_repo_loan_limit,
        usg_repo_loan_limit,
        mmt_repo_loan_limit
    FROM {{ source('sql2', 'counterpartyicreview') }}
),

counterparty_financials AS (
    SELECT
        counterparty_legal_name,
        date_of_latest_financials
    FROM {{ source('sql2', 'counterparty_financials') }}
),

counterparty_leverage_archive AS (
    SELECT
        counterparty_legal_name,
        asofdate AS leverage_asofdate,
        leverage
    FROM {{ source('sql2', 'counterpartyleveragearchive') }}
),

counterparty_nav_archive AS (
    SELECT
        counterparty_legal_name,
        nav,
        asofdate AS nav_asofdate
    FROM {{ source('sql2', 'counterpartynavarchive') }}
),

counterparty_aum_archive AS (
    SELECT
        counterparty_legal_name,
        aum,
        asofdate AS aum_asofdate
    FROM {{ source('sql2', 'counterpartyaumarchive') }}
),

counterparty_mra AS (
    SELECT
        counterparty_legal_name,
        nda,
        usg_mra,
        usg_formal_mta,
        usg_informal_mta,
        usg_pricing_sources,
        usg_dispute_rights,
        prime_mra,
        prime_formal_mta,
        prime_informal_mta,
        prime_pricing_sources,
        prime_dispute_rights,
        mmt_mra,
        mmt_formal_mta,
        mmt_informal_mta,
        mmt_pricing_sources,
        if_pricing_sources_inapplicable,
        mmt_dispute_rights,
        usg_mra_date,
        prime_mra_date,
        mm_mra_date,
        nda_date,
        nda_duration_years
    FROM {{ source('sql2', 'counterparty_mra') }}
),

counterparty_contacts AS (
    SELECT
        counterparty_legal_name,
        trading_contact1_name,
        trading_contact2_name,
        operations_contact1_name,
        operations_contact2_name
    FROM {{ source('sql2', 'counterparty_contacts') }}
)

SELECT
    c.counterparty_helix_name,
    c.counterparty_legal_name,
    c.manager,
    c.guarantor,
    c.place_of_business,
    c.group_type,
    c.entity_type,
    c.stock_ticker,
    c.short_description,
    c.public_or_private,
    c.risk_note,
    c.transparency_data,
    c.transparency_method,
    c.transparency_location,
    c.transparency_contact,
    c.transparency_frequency,
    c.auto_or_manual,

    -- Explicitly listing columns from counterparty_ic_review
    cir.last_ic_review_date,
    cir.initial_ic_review_date,
    cir.prime_repo_loan_limit,
    cir.usg_repo_loan_limit,
    cir.mmt_repo_loan_limit,

    -- Explicitly listing columns from counterparty_financials
    cf.date_of_latest_financials,

    -- Explicitly listing columns from counterparty_leverage_archive
    cla.leverage_asofdate,
    cla.leverage,

    -- Explicitly listing columns from counterparty_nav_archive
    cna.nav,
    cna.nav_asofdate,

    -- Explicitly listing columns from counterparty_aum_archive
    caa.aum,
    caa.aum_asofdate,

    -- Explicitly listing columns from counterparty_mra
    cm.nda,
    cm.usg_mra,
    cm.usg_formal_mta,
    cm.usg_informal_mta,
    cm.usg_pricing_sources,
    cm.usg_dispute_rights,
    cm.prime_mra,
    cm.prime_formal_mta,
    cm.prime_informal_mta,
    cm.prime_pricing_sources,
    cm.prime_dispute_rights,
    cm.mmt_mra,
    cm.mmt_formal_mta,
    cm.mmt_informal_mta,
    cm.mmt_pricing_sources,
    cm.if_pricing_sources_inapplicable,
    cm.mmt_dispute_rights,
    cm.usg_mra_date,
    cm.prime_mra_date,
    cm.mm_mra_date,
    cm.nda_date,
    cm.nda_duration_years,

    -- Explicitly listing columns from counterparty_contacts
    cc.trading_contact1_name,
    cc.trading_contact2_name,
    cc.operations_contact1_name,
    cc.operations_contact2_name

FROM counterparties c
LEFT JOIN counterparty_ic_review cir ON c.counterparty_legal_name = cir.counterparty_legal_name
LEFT JOIN counterparty_financials cf ON c.counterparty_legal_name = cf.counterparty_legal_name
LEFT JOIN counterparty_leverage_archive cla ON c.counterparty_legal_name = cla.counterparty_legal_name
LEFT JOIN counterparty_nav_archive cna ON c.counterparty_legal_name = cna.counterparty_legal_name
LEFT JOIN counterparty_aum_archive caa ON c.counterparty_legal_name = caa.counterparty_legal_name
LEFT JOIN counterparty_mra cm ON c.counterparty_legal_name = cm.counterparty_legal_name
LEFT JOIN counterparty_contacts cc ON c.counterparty_legal_name = cc.counterparty_legal_name

