def lab6_gcf(data, context):
        from google.cloud import pubsub_v1
        
        publisher = pubsub_v1.PublisherClient()
        topic_name = "projects/bdl2022labs/topics/lab6_pubsub"
        topic_id = "lab6_pubsub"
        
        future = publisher.publish(topic_name, bytes(data['name'] ,'utf-8'))
        future.result()