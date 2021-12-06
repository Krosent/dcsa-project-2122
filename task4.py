# Task 4; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import math

class MRFrobeniusNorm(MRJob):

    def steps(self):
        return [MRStep(mapper=self.read_file_mapper), MRStep(mapper=self.string_to_float_value_mapper)]

    def read_file_mapper(self, _, line):
        row = re.split(' ', line)
        yield None, row

    # transforms string values into float values
    def string_to_float_value_mapper(self, _, row):
        yield _, list(map(lambda col: float(col), row))


if __name__ == '__main__':
    MRFrobeniusNorm.run()
