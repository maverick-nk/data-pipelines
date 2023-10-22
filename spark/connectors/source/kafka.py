from connectors.source.reader import Reader

class KafkaReader(Reader):    
    def __init__(self, sparkSession, servers, topics, readOffset="earliest"):
        super().__init__(sparkSession)
        self.format = "kafka"
        self.servers = ",".join(servers)
        self.topics = ",".join(topics)
        self.readOffset = readOffset
    
    def readStream(self):
        return self.sparkSession.readStream\
            .format(self.format)\
            .option("kafka.bootstrap.servers", self.servers)\
            .option("subscribe", self.topics)\
            .option("startingOffsets", self.readOffset)\
            .load()
    
    def readBatch(self):
        return super().readBatch()