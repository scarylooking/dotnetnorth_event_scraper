import program
import uuid
import logging


logging.basicConfig(level=logging.INFO)

test_event = {}
test_context = type('obj', (object,), {'aws_request_id': str(uuid.uuid4())})()
print(program.lambda_handler(test_event, test_context))
