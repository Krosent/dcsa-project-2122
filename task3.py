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
                MRStep(reducer=self.group_measured_distances_reducer),
                MRStep(reducer=self.sort_and_k_nearest_neighbours_reducer),
                MRStep(reducer=self.group_measured_distances_reducer),
                MRStep(mapper=self.final_prediction_mapper)]

    def split_mapper(self, _, line):
        """
        Read File and Split each line to columns
        :param _:
        :param line: String
        :return: id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, species
        """
        row = re.split(',', line)
        yield None, row

    def values_mapper(self, _, row):
        """
        * Update min, max values for each feature.
        * Determine test set.
        :param _: None
        :param row: id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, species
        :return: id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, species
        """
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
                self.test.append((id, float(sepal_length_cm), float(sepal_width_cm), float(petal_length_cm), float(petal_width_cm)))

            yield None, row

    def update_min_and_max_values(self, col, position):
        """
        Update min amd max function
        :param col: String
        :param position: Integer
        :return: void
        """
        if col < self.minimum[position]:
            self.minimum[position] = col
        elif col > self.maximum[position]:
            self.maximum[position] = col

    def normalize_features_mapper(self, _, row):
        """
        Apply normalization formula on each feature. Formula:
        (feature row val - feature min val) / (feature max value - feature min value)
        :return: id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, species
        """
        # normalize each feature and return it in normal form
        row[1] = (float(row[1]) - self.minimum[0]) / (self.maximum[0] - self.minimum[0])
        row[2] = (float(row[2]) - self.minimum[1]) / (self.maximum[1] - self.minimum[1])
        row[3] = (float(row[3]) - self.minimum[2]) / (self.maximum[2] - self.minimum[2])
        row[4] = (float(row[4]) - self.minimum[3]) / (self.maximum[3] - self.minimum[3])
        yield None, row

    def measure_distance_reducer(self, _, rows):
        """
        Calculate distance between points
        :param _:
        :param rows: id, sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, species
        :return: test_row, (train_row, euclidean distance)
        """
        # calculate distance between each pairs
        rows = list(rows)

        for test_row in self.test:
            # get all rows from the sample
            filtered_rows = list(filter(lambda row: row != test_row, rows))
            for train_row in filtered_rows:
                result_without_square = 0.0
                for i in range(len(train_row) - 1):
                    diff = (float(test_row[i]) - float(train_row[i])) ** 2.0
                    result_without_square = result_without_square + diff
                eucl_dist = math.sqrt(result_without_square)
                if train_row[5] != '':
                    yield test_row, (train_row, eucl_dist)

    def group_measured_distances_reducer(self, test_row, train_rows):
        """
        Group train_rows by test_row
        """
        yield test_row, list(train_rows)

    def sort_and_k_nearest_neighbours_reducer(self, key, values):
        """
        Sort by closest neighbours and fetch k number of closest neighbours.
        :param key: test_row
        :param values: train_rows
        :return: test_row, 10 nearest neighbours
        """
        # sort by distances
        # flatten values list
        sorted_values = list(values)[0]
        sorted_values.sort(key=lambda val: val[1])
        # take only k number of neighbours
        sorted_values = sorted_values[:self.k_neighbours]
        for sort_value in sorted_values:
            yield key, sort_value

    def final_prediction_mapper(self, row, closest_neighbours):
        """
        Calculate which category occurs more often and make prediction based on that
        :param row: test_row
        :param closest_neighbours: list[train_rows] of k elements (15 by default)
        :return: row_id, prediction value as String
        """
        iris_versicolor = 0
        iris_setosa = 0
        iris_virginica = 0

        # calculate which category occurs more often
        for neighbour in closest_neighbours:
            if neighbour[0][5] == 'Iris-versicolor':
                iris_versicolor = iris_versicolor + 1
            elif neighbour[0][5] == 'Iris-virginica':
                iris_virginica = iris_virginica + 1
            elif neighbour[0][5] == 'Iris-setosa':
                iris_setosa = iris_setosa + 1

        # output the feature ID with predicted category
        if iris_versicolor >= iris_setosa and iris_versicolor >= iris_virginica:
            yield row[0], 'Iris-versicolor'
        elif iris_setosa >= iris_versicolor and iris_setosa >= iris_virginica:
            yield  row[0], 'Iris-Setosa'
        elif iris_virginica >= iris_versicolor and iris_virginica >= iris_setosa:
            yield row[0], 'Iris-virginica'


if __name__ == '__main__':
    MRKNearestNeighbour.run()
