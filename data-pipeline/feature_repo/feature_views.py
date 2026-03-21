from feast import FeatureView, Field
from feast.types import Float32, Int64, String
from datetime import timedelta
from disease_entities import region
from data_sources import disease_stats_source

disease_weekly_signals = FeatureView(  #raw CDC signals
    name="disease_weekly_signals",
    entities=[region],
    ttl=timedelta(weeks=52),
    schema=[
        Field(name="weekly_ili_cases",       dtype=Int64), 
        Field(name="total_patients_seen",    dtype=Int64),
        Field(name="provider_count",         dtype=Int64),
        Field(name="weighted_ili_pct",       dtype=Float32),
        Field(name="ili_pct",                dtype=Float32),
        Field(name="reporting_lag_days",     dtype=Int64),
    ],
    source=disease_stats_source,
    online=True,
)

disease_trend_features = FeatureView( #engineered trend features
    name="disease_trend_features",
    entities=[region],
    ttl=timedelta(weeks=520),
    schema=[
        Field(name="rolling_4wk_avg",            dtype=Float32),
        Field(name="rolling_4wk_std",            dtype=Float32),
        Field(name="week_over_week_pct_change",  dtype=Float32),
        Field(name="outbreak_risk_score",        dtype=Float32),
        Field(name="outbreak_flag",              dtype=Int64),
        Field(name="week_of_year",             dtype=Int64),
        Field(name="is_flu_season",           dtype=Int64),
        Field(name="week_sin",                dtype=Float32),
        Field(name="week_cos",                dtype=Float32),
    ],
    source=disease_stats_source,
    online=True,
)