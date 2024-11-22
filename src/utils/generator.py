from abc import ABC, abstractmethod

class Generator(ABC):
    
    def __init__(self, num_records):
        self.__num_records = num_records

    @abstractmethod
    def generate(self) -> list:
        pass