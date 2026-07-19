import asyncio
import datetime
import random
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json
import os
from pathlib import Path
from sensor_stream import make_event, SITES

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

EVENT_HUB_CONNECTION_STRING = os.environ.get("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME")

async def run():
    '''Creating a producer client to send messages to the event hub.'''
    
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME
    )
    for site in SITES:
        
        counter = 20
    
        async with producer:
            # Create a batch.
            while counter > 0:
                event_data_batch = await producer.create_batch()
                # Taking the current time
                current_ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Random power values
                random_consumption = random.uniform(10.0, 15.0)
                random_power = random.uniform(700.0, 800.0)
                random_pue = random.univorm(1.2, 1.4), 2

               
                event  = make_event(
                    site, 
                    timestamp_utc=current_ts,
                    consumption_kwh=random_consumption,
                    avg_power_kw=random_power,
                    pue=random_pue
                )

                payload = event.to_json_bytes()
                # Add events to the batch.
                event_data_batch.add(EventData(payload))

                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)
                await asyncio.sleep(3)
                print(payload)
                counter = counter - 1


asyncio.run(run())