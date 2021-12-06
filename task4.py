# Task 4; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import math

class MRFrobeniusNorm(MRJob):

    def steps(self):
        return [MRStep(mapper=self.read_file_mapper),
                MRStep(mapper=self.string_to_float_value_mapper),
                MRStep(reducer=self.sum_of_squares_reducer),
                MRStep(reducer=self.calculate_norm_reducer)]

    # read file line by line and split values into columns
    def read_file_mapper(self, _, line):
        row = re.split(' ', line)
        yield None, row

    # transforms string values into float values
    def string_to_float_value_mapper(self, _, row):
        yield _, list(map(lambda col: float(col), row))

    # calculate sum of squares
    def sum_of_squares_reducer(self, _, rows):
        for row in rows:
            cols = []
            for col in row:
                cols.append(col ** 2)
            yield _, sum(cols)

    # calculate F-norm
    def calculate_norm_reducer(self, _, rows):
        yield 'F-Norm Result:',  math.sqrt(sum(rows))

if __name__ == '__main__':
    MRFrobeniusNorm.run()
