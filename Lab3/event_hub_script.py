%pip install azure-eventhub nest-asyncio

import asyncio, datetime, random
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from sensor_stream import make_event, SITES


EH_NAME = "gabrielajaniszews786_eventhub"
EH_CONN_STR = dbutils.secrets.get("default2", "eventhub-con-str-gabriela")
ROUNDS = 20
SLEEP_S = 3

async def run():
    '''Creating a producer client to send messages to the event hub.'''
    
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EH_CONN_STR, eventhub_name=EH_NAME)
    sent = 0
    async with producer:
        for _ in range(ROUNDS):
            batch = await producer.create_batch()
            ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            for site in SITES:
                event = make_event(
                    site, 
                    timestamp_utc=ts,
                    consumption_kwh=random.uniform(10.0, 15.0),
                    avg_power_kw=random.uniform(700.0, 800.0),
                    pue = random.uniform(1.2, 1.4),
                )
                batch.add(EventData(event.to_json_bytes()))
                sent +=1
            
            await producer.send_batch(batch)
            await asyncio.sleep(SLEEP_S)
            print(f"Sent {sent} events from {len(SITES)} sites")
            

try:
    asyncio.run(run())
except RuntimeError:
    # Already inside a running loop (Databricks / Jupyter) -> patch and retry.
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(run())
