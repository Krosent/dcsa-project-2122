from mrjob.job import MRJob
import re

from mrjob.step import MRStep

class MRTopTenKeywordsForEachGenre(MRJob):

    def steps(self):
        return [MRStep(mapper=self.determine_titles_and_genres_mapper),
                MRStep(mapper=self.transform_titles_and_genres_into_lists_mapper),
                MRStep(mapper=self.titles_normalization_mapper)]

    def determine_titles_and_genres_mapper(self, _, line):

        splitted_columns = line.split(',')
        title = splitted_columns[1]
        genres = splitted_columns[2]
        yield "record", (title, genres)

    def transform_titles_and_genres_into_lists_mapper(self, _, input_row):
        yield "record", (input_row[0].split(' '), input_row[1].split('|'))

    # clean titles
    # title tags should not have numbers, prepositions, symbols
    def titles_normalization_mapper(self, _, input_row):
        keywords = input_row[0]
        genres = input_row[1]

        keywords = list(filter(str.isalpha, keywords))
        yield "record", (keywords, genres)

# example of bug: [["Tree"], [" 1951: A Portrait of James Dean (2012)\""]]

if __name__ == '__main__':
    MRTopTenKeywordsForEachGenre.run()
