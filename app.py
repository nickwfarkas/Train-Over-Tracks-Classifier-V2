from lambda_function import *

test_event = {
    "pathParameters": {
        "camera_name": "allen"
    }
}

lambda_handler(test_event, None)