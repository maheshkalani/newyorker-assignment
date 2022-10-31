from ingestion.schemas.business import schema as business
from ingestion.schemas.checkin import schema as checkin
from ingestion.schemas.review import schema as review
from ingestion.schemas.tip import schema as tip
from ingestion.schemas.user import schema as user

SCHEMA_INPUTS = {
    "business":
        {
            "input_schema": business.INPUT_SCHEMA,
            "map_columns_to_explode": business.MAP_COLUMNS_TO_EXPLODE,
            "columns_to_flatten": business.COLUMNS_TO_FLATTEN,
            "columns_to_drop": business.COLUMNS_TO_DROP
        },
    "checkin":
        {
            "input_schema": checkin.INPUT_SCHEMA
        },
    "review":
        {
            "input_schema": review.INPUT_SCHEMA,
            "formatting_columns": review.FORMATTING_COLUMNS
        },
    "tip":
        {
            "input_schema": tip.INPUT_SCHEMA
        },
    "user":
        {
            "input_schema": user.INPUT_SCHEMA
        }
}
