import os
from feast import FileSource

# Absolute path so feast resolves correctly regardless of working directory
_HERE = os.path.dirname(__file__)

ilinet_source = FileSource(
    name="ilinet_source",
    path=os.path.join(_HERE, "data/ilinet_features.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

covidcast_source = FileSource(
    name="covidcast_source",
    path=os.path.join(_HERE, "data/covidcast_features.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

flusurv_source = FileSource(
    name="flusurv_source",
    path=os.path.join(_HERE, "data/flusurv_features.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

noaa_source = FileSource(
    name="noaa_source",
    path=os.path.join(_HERE, "data/noaa_features.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)
