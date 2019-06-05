__VERSION__ = "0.2.0"

import json
from typing import Any

from dataclasses import dataclass, field, InitVar


@dataclass
class MessageMetadata:
    """
    Defines the metadata received from a message's body.
    """

    message: InitVar[Any]
    event_id: str = field(init=False)
    event_type: str = field(init=False)
    schema: Any = field(init=False)

    def __post_init__(self, message: Any):
        body: dict = json.loads(message.body)
        self.event_id = body.get("eventId", "")
        self.event_type = body.get("type", "")
        self.schema = body.get("schema")
