from abc import ABCMeta, abstractmethod
    
class Writer(metaclass=ABCMeta):
    def __init__(self):
        pass
    
    @abstractmethod
    def writeStream(self, dataframe):
        raise NotImplementedError

    @abstractmethod
    def writeBatch(self, dataframe):
        raise NotImplementedError