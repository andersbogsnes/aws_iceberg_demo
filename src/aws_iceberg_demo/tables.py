from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestamptzType, StringType, DecimalType, \
    LongType

event_table_schema = Schema(
    NestedField(1, "event_time", TimestamptzType(), required=True, doc="Time of the event in UTC at second precision"),
    NestedField(2, "event_type", StringType(), required=True, doc="Type of event. One of view, cart, remove_from_cart, purchase"),
    NestedField(3, "product_id", LongType(), required=True, doc="Id of the product"),
    NestedField(4, "category_id", LongType(), required=True, doc="Id of the category"),
    NestedField(5, "category_code", StringType(), required=False, doc="Category taxonomy if available"),
    NestedField(6, "brand", StringType(), required=False, doc="Lowercase brandname"),
    NestedField(7, "price", DecimalType(38, 2), required=True, doc="Price of item"),
    NestedField(8, "user_id", LongType(), required=True, doc="User Id"),
    NestedField(9, "user_session", StringType(), required=False, doc="ID for User's current session")
)
