# Task 4; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import math

class MRFrobeniusNorm(MRJob):

    def steps(self):
        return []


if __name__ == '__main__':
    MRFrobeniusNorm.run()
