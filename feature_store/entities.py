from feast import Entity
from feast.types import String

msno = Entity(
    name="msno",
    join_keys=["msno"],
    value_type=String,
)