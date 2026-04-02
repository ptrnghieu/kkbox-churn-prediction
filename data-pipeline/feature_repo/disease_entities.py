from feast import Entity, ValueType

region = Entity(
    name="region_id",
    value_type=ValueType.STRING,
    description="2-letter lowercase US state code (e.g. 'ca', 'ny')",
)
