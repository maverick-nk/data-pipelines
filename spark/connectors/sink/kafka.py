# Reference: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
from connectors.sink.writer import Writer


class KafkaWriter(Writer):
    def __init__(self):
        super().__init__()
    
    def writeStream(self, dataframe):
        return super().writeStream(dataframe)
    
    def writeBatch(self, dataframe):
        return super().writeBatch(dataframe)