from connectors.sink.writer import Writer

class PostgreSQLWriter(Writer):
    def __init__(self):
        super().__init__()

    def __write_to_db(self, dataframe, epoch_id):
        return dataframe.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgresDB") \
        .option("dbtable", "data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    def writeStream(self, dataframe):
        return dataframe.writeStream\
        .foreachBatch(self.__write_to_db)\
        .start()

    def writeBatch(self, dataframe):
        pass