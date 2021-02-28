import requests
import json
import boto3
import os
import logging
import utility

utility.configure_logger()


def lambda_handler(event, context):
    meetup_api = os.environ.get('meetup_api_url', None)
    if meetup_api is None:
        logging.error(f'meetup_api is not set')
        return

    bucket_name = os.environ.get('bucket_name', None)
    if bucket_name is None:
        logging.error(f'meetup_api is not set')
        return

    file_name = os.environ.get('file_name', None)
    if file_name is None:
        logging.error(f'file_name is not set')
        return

    logging.info(f'sending GET request to {meetup_api}...')

    r = requests.get(meetup_api)
    parsed_events = []

    logging.info(f'request complete')

    for event in r.json():
        event_name = event.get('name', None)
        event_date = event.get('local_date', None)
        event_time = event.get('local_time', None)
        event_is_online = event.get('is_online_event', None)

        if not event_name or not event_date or not event_time:
            logging.error(f'failed to process event: {event}')
            continue

        parsed_events.append({
            'name': event_name,
            'date': event_date,
            'time': event_time,
            'online': event_is_online
        })

        logging.info(f'processed "{event_name}" successfully')

    try:
        s3 = boto3.client('s3')
        s3.put_object(Body=(bytes(json.dumps(parsed_events).encode('UTF-8'))), Bucket=bucket_name, Key=file_name)
        logging.info(f'successfully written {file_name} to {bucket_name} with {len(parsed_events)} events')
    except Exception as e:
        logging.error(f'failed to write to bucket {bucket_name}: {str(e)}')
