from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    input_topic = sys.argv[1]
    output_topic = sys.argv[2]
except:
    print('Usage: python3 consumer <input_topic> <output_topic>')
    exit(1)

# Create consumer: Option 1 -- only consume new events
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

# Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
# consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([input_topic])

print(f'Listening on "{input_topic}", will forward processed events to "{output_topic}"...')

for msg in consumer:
    raw = msg.value.decode()
    print(f'Received: {raw}')

    # Process the event: transform the message
    processed = f'[PROCESSED] {raw.upper()}'

    producer.send(output_topic, value=processed.encode())
    print(f'Published to "{output_topic}": {processed}')
    producer.flush()
