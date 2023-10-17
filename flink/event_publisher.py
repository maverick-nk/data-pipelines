import argparse
import json
import logging
import sys
import uuid

from faker import Faker
from kafka import KafkaProducer
from time import sleep

# Instantiate faker to generate fake data stream
FAKE = Faker()
# Providing __name__ for genrator specific logs
LOGGER = logging.getLogger(__name__)

def generate_click_streams() -> str:
    data = {}
    data['id'] = uuid.uuid4().hex
    data['user_agent'] = FAKE.user_agent()
    data['ip'] = FAKE.ipv4()
    data['dow'] = FAKE.day_of_week()
    data['user'] = {}
    data['user']['email'] = FAKE.email()
    data['user']['city'] = FAKE.city()
    data['user']['country'] = FAKE.country()
    data['user']['name'] = FAKE.name()

    yield json.dumps(data, ensure_ascii=False).encode('utf-8')

class Producer():
    def __init__(self, hosts, no_of_records, topic, sleep_dur = 5):
        self.no_of_rec = no_of_records
        self.sleep_dur = sleep_dur
        self.topic = topic

        # Initialize producer
        self.producer = KafkaProducer(bootstrap_servers = hosts)

    def produce(self):
        for _ in range(self.no_of_rec):
            message = next(generate_click_streams())
            LOGGER.info(f"Sending message: {message}")
            
            # Send record
            self.producer.send(self.topic, value=message)

            sleep(self.sleep_dur)

if __name__ == "__main__":
    # Setting logger
    logging.basicConfig(
        filename='producer.log', 
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w',
    )
    LOGGER.setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser()        
    parser.add_argument(
        '--e_cnt',
        dest='e_cnt',
        type=int,
        required=True,
        help='No. of click events')
    parser.add_argument(
        '--sleep',
        dest='sleep',
        type=float,
        required=True,
        help='Sleep in seconds')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    
    LOGGER.info('Creating producer instance!!!')
    try:
        # Producing click events
        producer = Producer(
            hosts=['localhost:9094'],
            no_of_records=known_args.e_cnt,
            topic='clicks',
            sleep_dur=known_args.sleep
            )
        
        LOGGER.info('Producing click events now ...')
        producer.produce()
    except Exception as exc:
        LOGGER.info(f"Exception occurred: {exc}")
        
    LOGGER.info("Exiting generator.")
