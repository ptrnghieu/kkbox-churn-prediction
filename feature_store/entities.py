from feast import Entity
from feast.value_type import ValueType

msno = Entity(
    name="msno",
    join_keys=["msno"],
    value_type=ValueType.STRING,
)