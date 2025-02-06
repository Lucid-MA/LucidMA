{#{{#}
{#    config(#}
{#        materialized='table',  #}
{#        post-hook=[#}
{#            "{{ create_clustered_index(columns = ['tradepiece']) }}",  -- Clustered index on `tradepiece`#}
{#            "{{ create_nonclustered_index(columns = ['company']) }}",  -- Non-clustered index on `company`#}
{#            "{{ create_nonclustered_index(columns = ['statusmain']) }}"  -- Non-clustered index on `statusmain`#}
{#        ]#}
{#    )#}
{#}}#}
{##}
{#WITH source AS (#}
{#    SELECT#}
{#        *#}
{#    FROM#}
{#        {{ source('helix2', 'helix_raw__stream_HISTORY_TRADEPIECES') }}#}
{#),#}
{#json_data AS (#}
{#    SELECT#}
{#        JSON_VALUE(_airbyte_data, '$.ACCT_NUMBER') AS acct_number,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ALLOCATEDATE') AS DATETIME) AS allocatedate,#}
{#        JSON_VALUE(_airbyte_data, '$.AUCTION') AS auction,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.BASISTYPE') AS INT) AS basistype,#}
{#        JSON_VALUE(_airbyte_data, '$.BOINDICATOR') AS boindicator,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.BOOKDATE') AS DATETIME) AS bookdate,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.BREAKSIZE') AS FLOAT) AS breaksize,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.BROKERACCOUNT') AS INT) AS brokeraccount,#}
{#        JSON_VALUE(_airbyte_data, '$.BROKERNAME') AS brokername,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CALC_ON') AS BIT) AS calc_on,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CANCELPIECEREF') AS FLOAT) AS cancelpieceref,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CAPACITYCODE') AS INT) AS capacitycode,#}
{#        JSON_VALUE(_airbyte_data, '$.CLEARINGLOCATION') AS clearinglocation,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CLOSEDATE') AS DATETIME) AS closedate,#}
{#        JSON_VALUE(_airbyte_data, '$.COMMENTS') AS comments,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.COMPANY') AS INT) AS company,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CONTRAACCOUNT') AS INT) AS contraaccount,#}
{#        JSON_VALUE(_airbyte_data, '$.CONTRANAME') AS contraname,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.COUPON') AS FLOAT) AS coupon,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CURRENCY_MONEY') AS INT) AS currency_money,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CURRENCY_PAR') AS INT) AS currency_par,#}
{#        JSON_VALUE(_airbyte_data, '$.CUSIP') AS cusip,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.DATETIMEID') AS DATETIME) AS datetimeid,#}
{#        JSON_VALUE(_airbyte_data, '$.DELIVERY1') AS delivery1,#}
{#        JSON_VALUE(_airbyte_data, '$.DELIVERY2') AS delivery2,#}
{#        JSON_VALUE(_airbyte_data, '$.DELIVERY3') AS delivery3,#}
{#        JSON_VALUE(_airbyte_data, '$.DELIVERY4') AS delivery4,#}
{#        JSON_VALUE(_airbyte_data, '$.DELIVERY5') AS delivery5,#}
{#        JSON_VALUE(_airbyte_data, '$.DELIVERY6') AS delivery6,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.DELIVERYLOGIC') AS INT) AS deliverylogic,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.DELIVERYOPTION') AS INT) AS deliveryoption,#}
{#        JSON_VALUE(_airbyte_data, '$.DEPOSITORY') AS depository,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.DEPOSITORYID') AS INT) AS depositoryid,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.EFFECTIVEREPORATE') AS FLOAT) AS effectivereporate,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ENDDATE') AS DATETIME) AS enddate,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ENTERDATETIMEID') AS DATETIME) AS enterdatetimeid,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.FEE_CALC') AS INT) AS fee_calc,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.FEETYPE') AS INT) AS feetype,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.FUNDCODE') AS INT) AS fundcode,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.FX_FACTOR') AS FLOAT) AS fx_factor,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.FX_MONEY') AS FLOAT) AS fx_money,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.FX_PAR') AS FLOAT) AS fx_par,#}
{#        JSON_VALUE(_airbyte_data, '$.GENERAL_SPECIFIC') AS general_specific,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.HAIRCUT') AS FLOAT) AS haircut,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.HAIRCUTDIRECTION') AS INT) AS haircutdirection,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.HAIRCUTMETHOD') AS INT) AS haircutmethod,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.INDEXRATEOFFSET') AS FLOAT) AS indexrateoffset,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.INDEXRATETYPE') AS INT) AS indexratetype,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.INTEREST') AS FLOAT) AS interest,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISFAILED') AS BIT) AS isfailed,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISGSCC') AS BIT) AS isgscc,#}
{#        JSON_VALUE(_airbyte_data, '$.ISIN') AS isin,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISSUE') AS INT) AS issue,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISVISIBLE') AS BIT) AS isvisible,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.LEDGERACCOUNT') AS INT) AS ledgeraccount,#}
{#        JSON_VALUE(_airbyte_data, '$.LEDGERNAME') AS ledgername,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.MATURITY') AS DATETIME) AS maturity,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.MONEY') AS FLOAT) AS money,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.PAR') AS FLOAT) AS par,#}
{#        JSON_VALUE(_airbyte_data, '$.CUSIP') AS cusip,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEDATE') AS DATETIME) AS tradedate,#}
{#        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEPIECE') AS FLOAT) AS tradepiece#}
{#    FROM source#}
{#),#}
{#renamed AS (#}
{#    SELECT#}
{#        ROW_NUMBER() OVER (PARTITION BY tradepiece ORDER BY datetimeid DESC) AS row_num,#}
{#        acct_number,#}
{#        allocatedate,#}
{#        auction,#}
{#        basistype,#}
{#        boindicator,#}
{#        bookdate,#}
{#        breaksize,#}
{#        brokeraccount,#}
{#        brokername,#}
{#        calc_on,#}
{#        cancelpieceref,#}
{#        capacitycode,#}
{#        clearinglocation,#}
{#        closedate,#}
{#        comments,#}
{#        company,#}
{#        contraaccount,#}
{#        contraname,#}
{#        coupon,#}
{#        currency_money,#}
{#        currency_par,#}
{#        cusip,#}
{#        datetimeid,#}
{#        delivery1,#}
{#        delivery2,#}
{#        delivery3,#}
{#        delivery4,#}
{#        delivery5,#}
{#        delivery6,#}
{#        deliverylogic,#}
{#        deliveryoption,#}
{#        depository,#}
{#        depositoryid,#}
{#        effectivereporate,#}
{#        enddate,#}
{#        enterdatetimeid,#}
{#        fee_calc,#}
{#        feetype,#}
{#        fundcode,#}
{#        fx_factor,#}
{#        fx_money,#}
{#        fx_par,#}
{#        general_specific,#}
{#        haircut,#}
{#        haircutdirection,#}
{#        haircutmethod,#}
{#        indexrateoffset,#}
{#        indexratetype,#}
{#        interest,#}
{#        isfailed,#}
{#        isgscc,#}
{#        isin,#}
{#        issue,#}
{#        isvisible,#}
{#        ledgeraccount,#}
{#        ledgername,#}
{#        maturity,#}
{#        money,#}
{#        par,#}
{#        tradedate,#}
{#        tradepiece#}
{#    FROM json_data#}
{#)#}
{##}
{#SELECT#}
{#    *#}
{#FROM#}
{#    renamed#}
{#WHERE row_num = 1;#}
