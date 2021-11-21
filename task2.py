# Task 2; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRReverseLinkGraph(MRJob):
    def steps(self):
        return [MRStep(mapper=self.initial_mapper),
                MRStep(mapper=self.reverse_pairs),
                MRStep(reducer=self.target_reducer)
                ]

    # return any source, target
    def initial_mapper(self, _, line):
        if line[0] != '#':
            split_cols = re.split('\t', line)
            yield None, (split_cols[0], split_cols[1])

    # transform to target, sources
    def reverse_pairs(self, _, value):
        yield value[1], value[0]

    def target_reducer(self, target, sources):
        yield target, list(sources)


if __name__ == '__main__':
    MRReverseLinkGraph.run()
