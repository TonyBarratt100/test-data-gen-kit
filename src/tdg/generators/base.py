from dataclasses import dataclass
from faker import Faker
import random
@dataclass
class RNG:
    seed:int
    def __post_init__(self):
        random.seed(self.seed)
        self.fake=Faker(); self.fake.seed_instance(self.seed)
