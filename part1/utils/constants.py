MESSAGE_SCHEMA_STR = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Message",
    "type": "object",
    "properties": {
       "text": {"type": "string"},
       "somenumber": {"type": "integer"},
       "color": {"type": "integer"}
    },
    "required": ["text", "somenumber", "color"]
   }
"""

COLORS = ["red", "green", "blue"]
