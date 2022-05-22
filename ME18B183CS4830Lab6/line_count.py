import os

from google.cloud import storage
from google.cloud import pubsub_v1

client = storage.Client()
bucket = client.get_bucket('me18b183_bdl')

subscriber = pubsub_v1.SubscriberClient()

topic_name = 'projects/bdl2022labs/topics/lab6_pubsub'
subscription_name = 'projects/bdl2022labs/subscriptions/lab6_pubsub-sub'

def line_print(message):
    blob = bucket.get_blob(message.data.decode('utf-8'))
    text = blob.download_as_string()
    text = text.decode('utf-8')
    print('\nNumber of lines in the file', message.data.decode('utf-8'),' =', len(text.split('\n')))
    message.ack()

future = subscriber.subscribe(subscription_name, line_print)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()