import json
import logging
import sys
import pathlib

from pyflink.common import Types, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaSourceBuilder, FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, \
    JsonRowDeserializationSchema


def process_json_data(env):

    # define the source
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')]
    )

    def update_tel(data):
        # parse the json
        json_data = json.loads(data[1])
        json_data['tel'] += 1
        return data[0], json_data

    def filter_by_country(data):
        # the json data could be accessed directly, there is no need to parse it again using
        # json.loads
        return "China" in data[1]['addr']['country']

    ds.map(update_tel).filter(filter_by_country).print()

    #ds.map(lambda x: (x['dow'], 1)).print()
    
    # submit for execution
    env.execute()


def write_to_kafka(env):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    kafka_producer = FlinkKafkaProducer(
        topic='test_json_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()


def read_from_kafka_consumer(env):

    deserialization_schema = SimpleStringSchema()
    kafka_source = FlinkKafkaConsumer(
        topics='clicks',
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': 'localhost:9094',
            #'group.id':'test'
        }
    )
    
    def get_dow(data):
        json_data = json.loads(data)
        return (json_data['dow'], 1)

    # Consuming messages and printing
    ds = env.add_source(kafka_source)
    ds.map(get_dow).print()
    env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    
    working_dir = pathlib.Path().resolve()

    # Declare environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(f'file://{working_dir}/flink-sql-connector-kafka-3.0.0-1.17.jar')
        
    read_from_kafka_consumer(env)