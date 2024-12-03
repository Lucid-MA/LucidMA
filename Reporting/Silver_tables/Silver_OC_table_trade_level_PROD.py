from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    Date,
    DateTime,
    inspect,
)

from Utils.database_utils import engine_prod

engine = engine_prod
tb_name_trade = "oc_rates_trade_level"
tb_name_series = "oc_rates_series"


def create_trade_level_table_with_schema(tb_name, engine):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("oc_rates_id", String(255), primary_key=True),
        Column("report_date", Date),
        Column("fund", String),
        Column("Series", String),
        Column("Trade ID", Integer),
        Column("TradeType", String),
        Column("Start Date", Date),
        Column("End Date", DateTime),
        Column("Money", Float),
        Column("Counterparty", String),
        Column("Orig. Rate", Float),
        Column("Orig. Price", Float),
        Column("BondID", String),
        Column("Par/Quantity", Float),
        Column("HairCut", Float),
        Column("Spread", Float),
        Column("cp short", String),
        Column("Comments", String),
        Column("End Money", Float),
        Column("Product Type", String),
        Column("Collateral Type", String),
        Column("Factor", Float),
        Column("Clean_price", Float),
        Column("interest_accrued", Float),
        Column("dirty_price", Float),
        Column("Clean_collateral_MV", Float),
        Column("Collateral_MV", Float),
        Column("Days_Diff", Integer),
        Column("Trade_level_exposure", Float),
        Column("Clean_trade_level_exposure", Float),
        Column("CP_total_negative_exposure", Float),
        Column("CP_total_positive_exposure", Float),
        Column("Clean_CP_total_negative_exposure", Float),
        Column("Clean_CP_total_positive_exposure", Float),
        Column("CP_total_money", Float),
        Column("Trade_level_negative_exposure_percentage", Float),
        Column("Trade_level_positive_exposure_percentage", Float),
        Column("Clean_trade_level_negative_exposure_percentage", Float),
        Column("Clean_trade_level_positive_exposure_percentage", Float),
        Column("Clean_net_margin_MV", Float),
        Column("Net_margin_MV", Float),
        Column("Margin_RCV_allocation", Float),
        Column("Collateral_value_allocated", Float),
        Column("Clean_margin_RCV_allocation", Float),
        Column("Clean_collateral_value_allocated", Float),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def create_series_table_with_schema(tb_name, engine):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("oc_rates_id", String(255), primary_key=True),
        Column("report_date", Date),
        Column("fund", String),
        Column("series", String),
        Column("oc_rate", Float),
        Column("clean_oc_rate", Float),
        Column("collateral_mv", Float),
        Column("clean_collateral_mv", Float),
        Column("repo_money", Float),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


inspector = inspect(engine)
if not inspector.has_table(tb_name_trade):
    create_trade_level_table_with_schema(tb_name_trade, engine)

if not inspector.has_table(tb_name_series):
    create_series_table_with_schema(tb_name_series, engine)
