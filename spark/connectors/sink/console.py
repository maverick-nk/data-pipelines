from connectors.sink.writer import Writer


class ConsoleWriter(Writer):
    def __init__(self):
        super().__init__()
        self.format = "console"

    def writeStream(self, dataframe):
        return dataframe.writeStream.format(self.format).start()
    
    def writeBatch(self, dataframe):
        return super().writeBatch(dataframe)