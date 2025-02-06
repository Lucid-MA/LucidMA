{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_clustered_index(columns=['tradepiece']) }}",
            "{{ create_nonclustered_index(columns=['company']) }}",
            "{{ create_nonclustered_index(columns=['statusmain']) }}"
        ]
    })
}}

SELECT tradepieces.company,
       tradepieces.ledgername           AS series,
       tradepieces.tradepiece,
       tradepieces.tradedate            AS trade_date,
       tradepieces.startdate            AS start_date,
       tradepieces.enddate              AS close_date,
       tradepieces.enddate              AS end_date,
       tradepieces.statusmain,
       tradepiecexrefs.frontofficeid    AS alloc_of,
       tradepieces.PAR * CASE
                             WHEN tradepieces.tradetype IN (0, 22) THEN -1
                             ELSE 1 END AS par_quantity,
       tradepieces.ACCT_NUMBER          AS cp_short
FROM {{ ref('stg_helix__tradepieces') }} AS tradepieces
         INNER JOIN {{ ref('stg_helix__tradepiececalcdatas') }} AS tradepiececalcdatas
                    ON tradepiececalcdatas.tradepiece = tradepieces.tradepiece
         INNER JOIN {{ ref('stg_helix__tradecommissionpieceinfo') }} AS tradecommissionpieceinfo
                    ON tradecommissionpieceinfo.tradepiece = tradepieces.tradepiece
         INNER JOIN {{ ref('stg_helix__tradepiecexrefs') }} AS tradepiecexrefs
                    ON tradepieces.tradepiece = tradepiecexrefs.tradepiece
WHERE tradepieces.statusmain <> 6
  AND tradepieces.company IN (44, 45)
  AND tradepieces.LEDGERNAME = 'Master';
