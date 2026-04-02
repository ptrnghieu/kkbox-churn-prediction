from feast import FileSource

ilinet_source = FileSource(
    name="ilinet_source",
    path="data/ilinet_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

covidcast_source = FileSource(
    name="covidcast_source",
    path="data/covidcast_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

flusurv_source = FileSource(
    name="flusurv_source",
    path="data/flusurv_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

noaa_source = FileSource(
    name="noaa_source",
    path="data/noaa_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)
