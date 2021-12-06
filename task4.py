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

    def read_file_mapper(self, _, line):
        """
        Read file line by line and split values into columns
        :param _: None
        :param line: String
        :return: None, List[String]
        """
        row = re.split(' ', line)
        yield None, row

    def string_to_float_value_mapper(self, _, row):
        """
         Transforms string values into float values
        :param _: None
        :param row: List[String]
        :return: None, List[Float]
        """
        yield _, list(map(lambda col: float(col), row))

    def sum_of_squares_reducer(self, _, rows):
        """
        Calculate sum of squares
        :param _: None
        :param rows:
        :return:
        """
        for row in rows:
            cols = []
            for col in row:
                cols.append(col ** 2)
            yield _, sum(cols)

    def calculate_norm_reducer(self, _, rows):
        """
        Calculate F-norm
        :param _: None
        :param rows: List[Float]
        :return: String, Float
        """
        yield 'F-Norm Result:',  math.sqrt(sum(rows))

if __name__ == '__main__':
    MRFrobeniusNorm.run()
