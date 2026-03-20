from feast import FileSource

disease_stats_source = FileSource(
    name="disease_stats_source",
    # path is relative to where you run `feast apply`
    path="data/disease_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)