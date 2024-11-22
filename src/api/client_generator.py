from utils.generator import Generator
import random
from faker import Faker

class clientGenerator(Generator):
    ''''''
    def __init__(self, num_records: int) -> None:
        self.__schema = {'client_id',
                        'name',
                        'age',
                        'gender',
                        'address',
                        'email',
                        'phone_number',}
        self.__num_records = num_records
        self.fake = Faker('pt_BR')
        self.fake.seed_instance(1)
    
    def set_num_records(self, num_records : int) -> None:
        self.__num_records = num_records

    def get_schema(self) -> dict:
        return self.__schema
    
    def get_num_records(self) -> int:
        return self.__num_records
    
    def generate_client_id_seeds(self, num_seeds: int) -> list:
        '''Generate a list of client_id seeds'''
        return [self.fake.uuid4() for _ in range(num_seeds)]

    def generate(self) -> list:
        ''''''
        data = []
        random_client_id = self.generate_client_id_seeds(self.__num_records)
        rnd = random.Random(1)
        for i in range(self.__num_records):
            record = {}
            record['client_id'] = random_client_id[i]
            record['name'] = self.fake.name()
            record['age'] = self.fake.random_int(min=18, max=70)
            record['gender'] = rnd.choice(['M', 'F'])
            record['address'] = self.fake.address()
            record['email'] = self.fake.email()
            record['phone_number'] = self.fake.phone_number()
            data.append(record)
        return data

    