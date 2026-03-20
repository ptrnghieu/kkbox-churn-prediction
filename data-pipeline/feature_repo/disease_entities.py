from feast import Entity
from feast.value_type import ValueType

region = Entity(
    name="region",
    description="US state region code (e.g. 'ca', 'ny')",
    join_keys=["region_id"],
    value_type=ValueType.STRING,
    tags={"owner": "data_team", "domain": "disease_surveillance"}
)