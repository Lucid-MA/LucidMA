{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['trade_id']) }}",
        ]
    })

}}

WITH tradepieces AS (
  SELECT
    *
  FROM
    {{ ref('stg_helix__tradepieces') }}
),
tradecommissionpieceinfo AS (
  SELECT
    *
  FROM
    {{ ref('stg_helix__tradecommissionpieceinfo') }}
),
tradepiecexrefs AS (
  SELECT
    *
  FROM
    {{ ref('stg_helix__tradepiecexrefs') }}
),
tradepiececalcdatas AS (
  SELECT
    *
  FROM
    {{ ref('stg_helix__tradepiececalcdatas') }}
),
trade_query_part1 AS (
  SELECT
    CASE
      WHEN tradepiecexrefs.frontofficeid IS NOT NULL THEN TRY_CAST(tradepiecexrefs.frontofficeid AS FLOAT)
      WHEN tradecommissionpieceinfo.commissionvalue2 > 9999 THEN tradecommissionpieceinfo.commissionvalue2
      ELSE NULL
    END AS master_refid,
    tradepieces.tradepiece,
    tradepieces.company,
    LTRIM(RTRIM(tradepieces.ledgername)) AS ledgername,
    tradecommissionpieceinfo.commissionvalue2,
    COALESCE(TRY_CAST(tradepiecexrefs.frontofficeid AS FLOAT), NULL) AS frontofficeid,
    CASE
      WHEN tradepieces.company = 44 THEN 'USG'
      WHEN tradepieces.company = 45 THEN 'PRIME'
      WHEN tradepieces.company = 46 THEN 'MMT'
    END fund,
    UPPER(LTRIM(RTRIM(ledgername))) series,
    CASE
      WHEN NOT tradepieces.company = 45 THEN 1
      ELSE 0
    END is_also_master,
    tradepieces.tradetype trade_type,
    tradepieces.startdate start_date,
    CASE
      WHEN tradepieces.closedate IS NULL THEN tradepieces.enddate
      ELSE tradepieces.closedate
    END AS end_date,
    tradepieces.cusip security,
    tradepieces.isgscc is_buy_sell,
    tradepieces.par quantity,
    tradepieces.money,
    (tradepieces.money + tradepiececalcdatas.repointerest_unrealized + tradepiececalcdatas.repointerest_nbd) AS end_money,
    CASE
      WHEN ( tradepieces.company = 45 AND TRIM(UPPER(tradepieces.ledgername)) = 'MASTER')
        OR tradepieces.company IN ( 44, 46) 
        THEN 
        CASE
          WHEN tradepiecexrefs.frontofficeid IS NOT NULL THEN TRY_CAST(tradepiecexrefs.frontofficeid AS FLOAT)
          WHEN tradecommissionpieceinfo.commissionvalue2 > 9999 THEN tradecommissionpieceinfo.commissionvalue2
          ELSE 0
        END
      ELSE 0
    END roll_of,
    TRIM(UPPER(tradepieces.acct_number)) AS counterparty,
    tradepieces.depository,
    tradepieces.startdate,
    tradepieces.closedate,
    tradepieces.enddate
  FROM
    tradepieces
    JOIN tradepiececalcdatas
    ON tradepieces.tradepiece = tradepiececalcdatas.tradepiece
    JOIN tradecommissionpieceinfo
    ON tradepieces.tradepiece = tradecommissionpieceinfo.tradepiece
    JOIN tradepiecexrefs
    ON tradepieces.tradepiece = tradepiecexrefs.tradepiece
  WHERE
    1 = 1
    AND tradepieces.company IN (
      44,
      45
    )
    AND tradepieces.statusmain NOT IN (6)
),
masterpieces AS (
  SELECT
    tradepiece AS masterpiece,
    ledgername,
    par AS masterpar
  FROM
    tradepieces
),
trade_query_part2 AS (
  SELECT
    CONCAT(
      (
        CASE
          WHEN company IN ( 44, 46) THEN tradepiece
          WHEN TRIM(UPPER(trade_query_part1.ledgername)) = 'MASTER'
            AND ( company = 45) THEN tradepiece
          ELSE master_refid
        END
      ),
      ''
    ) trade_id,
    masterpiece,
    masterpar,
    trade_query_part1.*,
    CASE
      WHEN masterpiece IS NULL THEN 1
      WHEN COALESCE(TRY_CAST(masterpiece AS numeric), 0) <> 0 THEN quantity * 1.0 / masterpar
      ELSE 1
    END used_alloc
  FROM
    trade_query_part1
    LEFT JOIN masterpieces
    ON (
      trade_query_part1.master_refid IS NOT NULL
      AND trade_query_part1.master_refid = masterpieces.masterpiece
    )
),
trade_query_final AS (
  SELECT
    startdate AS report_date,
    CONCAT(trade_id,' TRANSMITTED') AS action_id,
    *
  FROM
    trade_query_part2
  WHERE trade_id NOT IN (202278)
  UNION ALL
  SELECT
    COALESCE(closedate, enddate) AS report_date,
    CONCAT(trade_id,' CLOSED') AS action_id,
    *
  FROM
    trade_query_part2
  WHERE (closedate IS NOT NULL OR enddate IS NOT NULL)
  AND trade_id NOT IN (202278)
),
final AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY report_date, fund, series, trade_id ORDER BY tradepiece DESC) AS row_rank,
    *
  FROM trade_query_final
)

SELECT 
  *,
  report_date AS orig_report_date
FROM final
--WHERE row_rank = 1
