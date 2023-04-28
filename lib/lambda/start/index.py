def handler(event, context):
    event.update({"items": [v for v in range(0, event["size"])]})
    return event
