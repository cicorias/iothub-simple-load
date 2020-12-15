
import asyncio
import os
from azure.eventhub import TransportType
from azure.eventhub.aio import EventHubProducerClient, EventHubConsumerClient, EventHubSharedKeyCredential


from environs import Env


async def run():
    env = Env()
    env.read_env() 


    with env.prefixed("EVENT_HUB_"):
        CONNECTION_STRING =  env('CONN_STR')
        FULLY_QUALIFIED_NAMESPACE = env('HOSTNAME')
        EVENTHUB_NAME = env('NAME')
        SAS_POLICY = env('SAS_POLICY')
        SAS_KEY = env('SAS_KEY')
        CONSUMER_GROUP = env('CONSUMER_GROUP', "$Default")


    producer_client = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME,  # EventHub name should be specified if it doesn't show up in connection string.
        logging_enable=False,  # To enable network tracing log, set logging_enable to True.
        retry_total=3,  # Retry up to 3 times to re-do failed operations.
        transport_type=TransportType.Amqp  # Use Amqp as the underlying transport protocol.
    )


    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,  # EventHub name should be specified if it doesn't show up in connection string.
        logging_enable=False,  # To enable network tracing log, set logging_enable to True.
        retry_total=3,  # Retry up to 3 times to re-do failed operations.
        transport_type=TransportType.Amqp  # Use Amqp as the underlying transport protocol.
    )


    async with producer_client:
        p = await producer_client.get_partition_ids()
        print(p)

    print('foobar')

    asyncio.create_task(run()).result()