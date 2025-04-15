import json

from src.models import ItemType


def lambda_handler(event, context):
    # fmt = {
    #     "id": "123",
    #     "refresh_token": "",
    #     "spotify_data": [
    #         {
    #             "top_items": [
    #                 {"id": "123", "position": 1}
    #             ],
    #             "item_type": "artists",
    #             "time_range": "long_term"
    #         }
    #     ]
    # }

    # 1. Extract user_id, refresh_token and spotify_data from event records
    # 2. Update user's spotify refresh_token in DB
    # 3. Add user's spotify top items to DB

    record = event["Records"][0]
    data = json.loads(record["body"])
    user_id = data["id"]
    refresh_token = data["refresh_token"]
    spotify_data = data["spotify_data"]

    if refresh_token is not None:
        # update refresh_token in DB
        pass

    for entry in spotify_data:
        top_items = entry["top_items"]
        item_type = entry["item_type"]
        time_range = entry["time_range"]

        # store top items in db

    print('hello, world!')
