# Task 2; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRReverseLinkGraph(MRJob):

    def steps(self):
        return [MRStep(mapper=self.initial_mapper),
                MRStep(mapper=self.reverse_pairs_mapper),
                MRStep(reducer=self.target_reducer)]

    def initial_mapper(self, _, line):
        """
        Load and Split Data by Tab. Each row has two columns
        :param _: None
        :param line: String
        :return: None, (String, String)
        """
        if line[0] != '#':
            split_cols = re.split('\t', line)
            yield None, (split_cols[0], split_cols[1])

    def reverse_pairs_mapper(self, _, value):
        """
        Reverse graph in a way that we have target -> source instead of source -> target
        :param _: None
        :param value: (String, String)
        :return: (String, String) -> target, sources
        """
        yield value[1], value[0]

    def target_reducer(self, target, sources):
        """
        Combine all sources for each target
        :param target: String
        :param sources: List[String] -> List of sources
        :return: target, list of sources for the target
        """
        yield target, list(sources)


if __name__ == '__main__':
    MRReverseLinkGraph.run()
