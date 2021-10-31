# Task 2; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRReverseLinkGraph(MRJob):
    def steps(self):
        return [MRStep(mapper=self.initial_mapper)]

    def initial_mapper(self, _, line):
        if line[0] != '#':
            split_cols = re.split('\t', line)
            yield None, (split_cols[0], split_cols[1])


if __name__ == '__main__':
    MRReverseLinkGraph.run()