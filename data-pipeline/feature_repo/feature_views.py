from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float32, Int64

from disease_entities import region
from data_sources import ilinet_source, covidcast_source, flusurv_source, noaa_source


# ---------------------------------------------------------------------------
# ILINet — CDC weekly clinical surveillance, 2010-present
# Source: Delphi Epidata fluview API, state-level, weekly
# ---------------------------------------------------------------------------

ilinet_signals = FeatureView(
    name="ilinet_signals",
    entities=[region],
    ttl=timedelta(weeks=52),
    schema=[
        Field(name="weekly_ili_cases",      dtype=Int64),
        Field(name="total_patients_seen",   dtype=Int64),
        Field(name="provider_count",        dtype=Int64),
        Field(name="weighted_ili_pct",      dtype=Float32),
        Field(name="ili_pct",               dtype=Float32),
        Field(name="reporting_lag_days",    dtype=Int64),
    ],
    source=ilinet_source,
    online=True,
)

ilinet_trend_features = FeatureView(
    name="ilinet_trend_features",
    entities=[region],
    ttl=timedelta(weeks=104),  # 2 years — enough history for z-score baseline
    schema=[
        Field(name="rolling_4wk_avg",           dtype=Float32),
        Field(name="rolling_4wk_std",           dtype=Float32),
        Field(name="week_over_week_pct_change",  dtype=Float32),
        Field(name="outbreak_risk_score",        dtype=Float32),
        Field(name="outbreak_flag",              dtype=Int64),
        Field(name="week_of_year",               dtype=Int64),
        Field(name="is_flu_season",              dtype=Int64),
        Field(name="week_sin",                   dtype=Float32),
        Field(name="week_cos",                   dtype=Float32),
    ],
    source=ilinet_source,
    online=True,
)


# ---------------------------------------------------------------------------
# COVIDcast — Delphi multi-signal daily surveillance, 2020-present
# Source: Delphi Epidata covidcast API, county-level → aggregated to state
# ---------------------------------------------------------------------------

covidcast_signals = FeatureView(
    name="covidcast_signals",
    entities=[region],
    ttl=timedelta(days=90),
    schema=[
        Field(name="chng_smoothed_outpatient_flu",      dtype=Float32),
        Field(name="doctor_visits_smoothed_adj_cli",    dtype=Float32),
        Field(name="fb_survey_smoothed_hh_cmnty_cli",  dtype=Float32),
        Field(name="rolling_7day_avg_chng",             dtype=Float32),
        Field(name="rolling_7day_avg_dv",               dtype=Float32),
        Field(name="week_of_year",                      dtype=Int64),
        Field(name="is_flu_season",                     dtype=Int64),
    ],
    source=covidcast_source,
    online=True,
)


# ---------------------------------------------------------------------------
# FluSurv-NET — CDC lab-confirmed flu hospitalization rates, 2010-present
# Source: Delphi Epidata flusurv API, HHS region-level → expanded to state
# Replaces HHS Protect (discontinued May 2023 after COVID PHE ended)
# ---------------------------------------------------------------------------

flusurv_signals = FeatureView(
    name="flusurv_signals",
    entities=[region],
    ttl=timedelta(weeks=104),
    schema=[
        Field(name="rate_overall",                  dtype=Float32),
        Field(name="rate_age_0",                    dtype=Float32),  # 0–4 yrs
        Field(name="rate_age_1",                    dtype=Float32),  # 5–17 yrs
        Field(name="rate_age_2",                    dtype=Float32),  # 18–49 yrs
        Field(name="rate_age_3",                    dtype=Float32),  # 50–64 yrs
        Field(name="rate_age_4",                    dtype=Float32),  # 65+ yrs
        Field(name="rolling_4wk_avg_hosp_rate",     dtype=Float32),
        Field(name="week_over_week_pct_change_hosp", dtype=Float32),
        Field(name="week_of_year",                  dtype=Int64),
        Field(name="is_flu_season",                 dtype=Int64),
    ],
    source=flusurv_source,
    online=True,
)


# ---------------------------------------------------------------------------
# NOAA Weather — daily temperature & precipitation, station → state average
# Source: NOAA Climate Data Online (CDO) API
# ---------------------------------------------------------------------------

noaa_signals = FeatureView(
    name="noaa_signals",
    entities=[region],
    ttl=timedelta(days=365),
    schema=[
        Field(name="tmax",                  dtype=Float32),
        Field(name="tmin",                  dtype=Float32),
        Field(name="tavg",                  dtype=Float32),
        Field(name="prcp",                  dtype=Float32),
        Field(name="rolling_7day_avg_tmax", dtype=Float32),
        Field(name="rolling_7day_avg_tmin", dtype=Float32),
        Field(name="rolling_7day_avg_prcp", dtype=Float32),
        Field(name="heating_degree_days",   dtype=Float32),
        Field(name="cooling_degree_days",   dtype=Float32),
        Field(name="week_of_year",          dtype=Int64),
        Field(name="is_flu_season",         dtype=Int64),
    ],
    source=noaa_source,
    online=True,
)
