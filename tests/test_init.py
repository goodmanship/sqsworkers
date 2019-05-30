from sqsworkers import MessageMetadata
from types import SimpleNamespace
import json


def test_metadata():
    event_id, type_, schema = 0, "type", "schema"

    message = SimpleNamespace(
        body=json.dumps({"eventId": event_id, "type": type_, "schema": schema})
    )

    message_metadata = MessageMetadata(message)

    assert message_metadata.event_id == event_id
    assert message_metadata.event_type == type_
    assert message_metadata.schema == schema
