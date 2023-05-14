from lambda_function import *

test_event = {
    "camera_name": "allen"
}

lambda_handler(test_event, None)