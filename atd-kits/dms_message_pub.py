"""
Extract DMS message from KITS database and upload to Data Tracker (Knack).
"""
import os
import re

import knackpy

import utils.kits_utils as kits_utils
import utils.logging as logging

KITS_CREDENTIALS = {
    "server": os.getenv("KITS_SERVER"),
    "user": os.getenv("KITS_USER"),
    "password": os.getenv("KITS_PASSWORD"),
    "database": os.getenv("KITS_DATABSE"),
}

KNACK_CONFIG = {
    "scene": "scene_569",
    "view": "view_1564",
    "obj": "object_109",
    "app_id": os.getenv("KNACK_APP_ID"),
    "api_key": os.getenv("KNACK_API_KEY"),
}

# Knack Field definitions for KITS fields
KITS_FIELD_MAPPING = {
    "KITS_ID": "field_1639",
    "DMS_MESSAGE": "field_1794",
    "MESSAGE_TIME": "field_1795",
}

OUTPUT_FIELDS = ["id", "field_1639", "field_1794", "field_1795"]


def cleanup_dms_message(message):
    #  removes DMS formatting artifacts, likely used by the device for text formatting

    # New line character and space
    message = message.replace("[np]", "\n")
    message = message.replace("[nl]", " ")

    # Remove all other formatting items that are stored between brackets []
    message = remove_text_in_brackets(message)
    return message


def remove_text_in_brackets(text):
    pattern = r"\[.*?\]"  # Regular expression pattern to match text between []
    clean_text = re.sub(pattern, "", text)
    return clean_text


def main():
    kits_query = """
        SELECT DMSID as KITS_ID
        ,Multistring as DMS_MESSAGE
        ,LastUpdated as MESSAGE_TIME
        FROM [KITS].[DMS_RealtimeData]
        """

    kits_data = kits_utils.data_as_dict(KITS_CREDENTIALS, kits_query)
    logger.info(f"{len(kits_data)} records returned from KITS DB.")

    # Datetime formatting and message cleanup  for Knack
    for record in kits_data:
        record["MESSAGE_TIME"] = record["MESSAGE_TIME"].strftime("%m/%d/%Y %H:%M")
        record["DMS_MESSAGE"] = cleanup_dms_message(record["DMS_MESSAGE"])

    # Get current Knack Data
    kwargs = {"scene": KNACK_CONFIG["scene"], "view": KNACK_CONFIG["view"]}
    data = knackpy.api.get(
        app_id=KNACK_CONFIG["app_id"], api_key=KNACK_CONFIG["api_key"], **kwargs
    )
    logger.info(f"{len(data)} records returned from Knack.")

    # Match Knack and KITS data on the KITS_ID field and return data that needs to be updated
    updated_data = []
    if kits_data:
        for rec in data:
            kits_id = rec[KITS_FIELD_MAPPING["KITS_ID"]]
            # Finding possible matching record
            matching_kits_record = [
                item for item in kits_data if item.get("KITS_ID") == kits_id
            ]
            if len(matching_kits_record) == 1:
                if (
                    matching_kits_record[0]["DMS_MESSAGE"].replace("\n", "<br />")
                    != rec[KITS_FIELD_MAPPING["DMS_MESSAGE"]]
                ):
                    rec[KITS_FIELD_MAPPING["DMS_MESSAGE"]] = matching_kits_record[0][
                        "DMS_MESSAGE"
                    ]
                    rec[KITS_FIELD_MAPPING["MESSAGE_TIME"]] = matching_kits_record[0][
                        "MESSAGE_TIME"
                    ]
                    # Only need the output fields
                    updated_data.append(
                        {key: rec[key] for key in OUTPUT_FIELDS if key in rec}
                    )
    else:
        logger.info("No data returned from KITS DB")
        return 0

    logger.info(f"Updating {len(updated_data)} records in Knack.")
    # Updating knack records
    for record in updated_data:
        res = knackpy.api.record(
            app_id=KNACK_CONFIG["app_id"],
            api_key=KNACK_CONFIG["api_key"],
            obj=KNACK_CONFIG["obj"],
            method="update",
            data=record,
        )

    return updated_data


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    main()
