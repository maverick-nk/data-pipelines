from connectors.sink.writer import Writer

class PostgreSQLWriter(Writer):
    def __init__(self):
        super().__init__()

    def __write_to_db(self, dataframe, epoch_id):
        # TODO: .option("onConflict", "id") # For upsert behavior
        # Check batch inserts to reduce connections. 
        # .option("batchsize", "1000") \
        # .option("rewriteBatchedInserts", "true") \
        return dataframe.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgresDB") \
        .option("dbtable", "data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "1000") \
        .option("rewriteBatchedInserts", "true") \
        .mode("append") \
        .save()

    def writeStream(self, dataframe):
        # TODO: Need to check the checkpoint for exactly once processing
        return dataframe.writeStream\
        .foreachBatch(self.__write_to_db)\
        .start()

    def writeBatch(self, dataframe):
        pass