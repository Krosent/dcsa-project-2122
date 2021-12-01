# Task 2; DCSA course at VUB; (c) Artyom Kuznetsov
import numbers

from mrjob.job import MRJob
from mrjob.step import MRStep
from numbers import Number
import re
import pandas as pd
import math


class MRKNearestNeighbour(MRJob):
    # store maximum values
    maximum = [-math.inf for _ in range(4)]
    # store minimum values
    minimum = [math.inf for _ in range(4)]
    # store test data
    test = []

    def steps(self):
        return [MRStep(mapper=self.split_mapper),
                MRStep(mapper=self.values_mapper),
                MRStep(mapper=self.normalize_features_mapper),
                MRStep(reducer=self.measure_distance_reducer)
                # MRStep(combiner=self._combiner),
                # MRStep(reducer=self.shitty_reducer)
                ]

    def split_mapper(self, _, line):
        row = re.split(',', line)
        yield None, row

    def values_mapper(self, _, row):
        # escape first row which is columns names
        if row[0].isnumeric():
            # features
            id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, species = row

            # update min and max values
            self.update_min_and_max_values(float(sepal_length_cm), 0)
            self.update_min_and_max_values(float(sepal_width_cm), 1)
            self.update_min_and_max_values(float(petal_length_cm), 2)
            self.update_min_and_max_values(float(petal_width_cm), 3)

            # append record if it has not species identification
            if species == "":
                self.test.append((id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm))

            # determine min, max for each feature
            yield None, row

    def update_min_and_max_values(self, sepal_length_cm, position):
        if sepal_length_cm < self.minimum[position]:
            self.minimum[position] = sepal_length_cm
        elif sepal_length_cm > self.maximum[position]:
            self.maximum[position] = sepal_length_cm

    def normalize_features_mapper(self, _, row):
        # normalize each feature and return it in normal form
        row[1] = (float(row[1]) - self.minimum[0]) / (self.maximum[0] - self.minimum[0])
        row[2] = (float(row[2]) - self.minimum[1]) / (self.maximum[1] - self.minimum[1])
        row[3] = (float(row[3]) - self.minimum[2]) / (self.maximum[2] - self.minimum[2])
        row[4] = (float(row[4]) - self.minimum[3]) / (self.maximum[3] - self.minimum[3])
        yield None, row

    # TODO: Implement Euclidean Distance
    def measure_distance_reducer(self, _, rows):
        rows
        yield rows


    # def euclidean_distance(row1, row2):
    #     distance = 0.0
    #     for i in range(len(row1) - 1):
    #         distance += (row1[i] - row2[i]) ** 2
    #     return sqrt(distance)

    # def _combiner(self, _, rows2):
    #     # yield None, ('kek', (l_rows[0], id_min, id_max))
    #     yield 0, list(rows2)
    #
    # def shitty_reducer(self, row, rows):
    #     yield row, list(rows)
    #     # yield row, (min(list(rows)), max(list(rows)))


if __name__ == '__main__':
    MRKNearestNeighbour.run()
