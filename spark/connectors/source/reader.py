from abc import ABCMeta, abstractmethod

class Reader(metaclass=ABCMeta):
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
    
    @abstractmethod
    def readStream(self):
        raise NotImplementedError

    @abstractmethod
    def readBatch(self):
        raise NotImplementedError