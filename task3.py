# Task 3; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import math

class MRKNearestNeighbour(MRJob):
    # store maximum values
    maximum = [-math.inf for _ in range(4)]
    # store minimum values
    minimum = [math.inf for _ in range(4)]
    # store test data
    test = []
    k_neighbours = 15

    def steps(self):
        return [MRStep(mapper=self.split_mapper),
                MRStep(mapper=self.values_mapper),
                MRStep(mapper=self.normalize_features_mapper),
                MRStep(reducer=self.measure_distance_reducer),
                MRStep(mapper=self.closest_neighbours_mapper),
                #MRStep(reducer=self.sort_reducer)
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

    def measure_distance_reducer(self, _, rows):
        # calculate distance between each pairs
        rows = list(rows)
        for i_row in rows:
            filtered_rows = list(filter(lambda row: row != i_row, rows))
            for j_row in filtered_rows:
                result_without_square = 0.0
                for i in range(len(i_row) - 1):
                    diff = (float(i_row[i]) - float(j_row[i])) ** 2.0
                    result_without_square = result_without_square + diff
                eucl_dist = math.sqrt(result_without_square)
                yield None, (j_row, i_row, eucl_dist)

    def closest_neighbours_mapper(self, _, row):
        j_row, i_row, eucl_dist = row
        yield (i_row[0], j_row[0]), eucl_dist

    def sort_reducer(self, key, values):
        yield key, sorted(values)

if __name__ == '__main__':
    MRKNearestNeighbour.run()
