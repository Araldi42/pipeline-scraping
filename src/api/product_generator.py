from utils.generator import Generator
import numpy as np
import random
from faker import Faker

class productGenerator(Generator):
    ''''''
    def __init__(self, num_records: int) -> None:
        self.__schema = {'product_id',
                         'name',
                         'price',
                         'category',
                         'quantity',
                         'description',}
        self.__num_records = num_records
        self.fake = Faker('pt_BR')
        self.fake.seed_instance(1)

    def set_num_records(self, num_records : int) -> None:
        self.__num_records = num_records

    def get_schema(self) -> dict:
        return self.__schema
    
    def get_num_records(self) -> int:
        return self.__num_records

    def generate_product_id_seeds(self, num_seeds: int) -> list:
        '''Generate a list of product_id seeds'''
        return [self.fake.uuid4() for _ in range(num_seeds)]
    
    def generate(self) -> list:
        ''''''
        data = []
        seed = ['eletronics', 'clothing', 'food', 'books']
        random_product_id = self.generate_product_id_seeds(self.__num_records)
        rnd = random.Random(1)
        for i in range(self.__num_records):
            
            record = {}
            record['product_id'] = random_product_id[i]
            record['name'] = self.fake.word() + ' ' + self.fake.word()
            record['price'] = self.fake.random_int(min=20, max=1000)
            record['category'] = rnd.choice(seed)
            record['quantity'] = self.fake.random_int(min=50, max=1000)
            data.append(record)
        return data