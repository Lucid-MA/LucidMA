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
    COALESCE(
      CASE
        WHEN tradecommissionpieceinfo.commissionvalue2 = 0 THEN NULL
        ELSE tradecommissionpieceinfo.commissionvalue2
      END,
      COALESCE(TRY_CAST(tradepiecexrefs.frontofficeid AS FLOAT), NULL)
    ) AS master_refid,
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
    (
      tradepieces.money + tradepiececalcdatas.repointerest_unrealized + tradepiececalcdatas.repointerest_nbd
    ) end_money,
    CASE
      WHEN (
        tradepieces.company = 45
        AND LTRIM(RTRIM(tradepieces.ledgername)) = 'Master'
      )
      OR tradepieces.company IN (
        44,
        46
      ) THEN COALESCE(
        CASE
          WHEN tradecommissionpieceinfo.commissionvalue2 = 0 THEN NULL
          ELSE tradecommissionpieceinfo.commissionvalue2
        END,
        COALESCE(TRY_CAST(tradepiecexrefs.frontofficeid AS FLOAT), '')
      )
      ELSE ''
    END roll_of,
    CASE
      WHEN LTRIM(RTRIM(tradepieces.acct_number)) = '400CAPTX' THEN 'TEX'
      ELSE LTRIM(RTRIM(tradepieces.acct_number))
    END counterparty,
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
    par AS masterpar
  FROM
    tradepieces
),
trade_query_final AS (
  SELECT
    CONCAT(
      (
        CASE
          WHEN company IN(
            44,
            46
          ) THEN tradepiece
          WHEN LTRIM(RTRIM(ledgername)) = 'Master'
          AND (
            company = 45
          ) THEN tradepiece
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
      trade_query_part1.master_refid = masterpieces.masterpiece
    )
),
final AS (
  SELECT
    startdate AS report_date,
    CONCAT(trade_id,' TRANSMITTED') AS action_id,
    *
  FROM
    trade_query_final
  UNION ALL
  SELECT
    COALESCE(closedate, enddate) AS report_date,
    CONCAT(trade_id,' CLOSED') AS action_id,
    *
  FROM
    trade_query_final
  WHERE closedate IS NOT NULL OR enddate IS NOT NULL
)

SELECT * FROM final
