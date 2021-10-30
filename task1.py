# Task 1; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re

# (!) Uncomment it and run first time in order to download stop-words to your machine
import nltk

nltk.download('stopwords')
nltk.download('punkt')


class MRTopTenKeywordsForEachGenre(MRJob):

    def steps(self):
        """
        We execute our application in several steps described below
        :return list of MRSteps:
        """
        return [MRStep(mapper=self.determine_titles_and_genres_mapper),
                MRStep(mapper=self.transform_titles_and_genres_into_lists_mapper),
                MRStep(mapper=self.titles_normalization_mapper),
                MRStep(mapper=self.keyword_and_tags_mapper, reducer=self.calculate_occurances_reducer),
                MRStep(mapper=self.sort_mapper, reducer=self.sort_reducer)]

    def determine_titles_and_genres_mapper(self, _, line):
        """
        Read csv file, split into columns and output title and genres
        :param _:
        :param line:
        :return (title, genres):
        """
        if '\"' in line:
            split_cols = re.split(',\"|\",', line)
        else:
            split_cols = re.split(',', line)

        title = split_cols[1]
        genres = split_cols[2]
        yield None, (title, genres)

    def transform_titles_and_genres_into_lists_mapper(self, _, input_row):
        """
        Transform genres string into a list of genres
        :param _:
        :param input_row:
        :return list of keywords and list of genres:
        """
        yield None, (word_tokenize(input_row[0]), input_row[1].split('|'))

    def titles_normalization_mapper(self, _, input_row):
        """
        Normalize titles. Removal of numbres, prepositions, special symbols and other stop words
        :param _:
        :param input_row:
        :return filtered list of keywords and list of genres:
        """
        keywords = input_row[0]
        genres = input_row[1]

        keywords = list(filter(str.isalpha, keywords))
        keywords_filtered = []

        stop_words = set(stopwords.words('english'))
        for keyword in keywords:
            keyword = str.lower(keyword)
            if keyword not in stop_words:
                keywords_filtered.append(keyword)

        yield None, (keywords_filtered, genres)

    def keyword_and_tags_mapper(self, _, input_row):
        """
        Transform data into a key which is a combination of genre and keyword and the value, which is one.
        This value then will be used to calculate the occurrence of the same keyword.
        Also, we remove keywords that are not having any genres.
        :param _:
        :param input_row:
        :return (genre, keyword) as a key ; 1 as value:
        """
        keywords = input_row[0]
        genres = input_row[1]

        for keyword in keywords:
            for genre in genres:
                if genre != '(no genres listed)' and genre != 'genres':
                    yield (genre, keyword), 1

    def calculate_occurances_reducer(self, key, values):
        """
        Calculate occurrences of keyword for the same genre.
        :param key:
        :param values:
        :return (genre, keyword) as key and Int Value as the occurrence of keyword:
        """
        yield key, sum(values)

    def sort_mapper(self, key, value):
        """
        Remap our data so we have genre as a key and combination of key and int value as a value for further reduction.
        :param key:
        :param value:
        :return genre as a key and combination of key and int value as a value:
        """
        yield key[0], (value, key[1])

    def sort_reducer(self, key, values):
        """
        Reduce our data to the genre name and top 10 occurred keywords.
        :param key:
        :param values:
        :return genre as a key ; list of keywords as value:
        """
        yield key, sorted(values, reverse=True)[:10]


if __name__ == '__main__':
    MRTopTenKeywordsForEachGenre.run()
