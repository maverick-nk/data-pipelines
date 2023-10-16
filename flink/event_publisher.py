import argparse
import uuid
import json

from faker import Faker
from kafka import KafkaProducer

# Instantiate faker to generate fake data stream
FAKE = Faker()

def generate_click_streams() -> str:
    pass

def generate_impression_streams() -> str:
    pass

def publish_event() -> None:
    pass

if __name__ == "__main__":
    
    click_stream = generate_click_streams()
    impression_stream = generate_impression_streams()
