# Task 1; DCSA course at VUB; (c) Artyom Kuznetsov

from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords

# (!) Uncomment it and run first time in order to download stop-words to your machine
# import nltk
# nltk.download('stopwords')

class MRTopTenKeywordsForEachGenre(MRJob):

    def steps(self):
        return [MRStep(mapper=self.determine_titles_and_genres_mapper),
                MRStep(mapper=self.transform_titles_and_genres_into_lists_mapper),
                MRStep(mapper=self.titles_normalization_mapper),
                MRStep(mapper=self.keyword_and_tags_mapper)
                ]

    @staticmethod
    def determine_titles_and_genres_mapper(_, line):
        splitted_columns = line.split(',')
        title = splitted_columns[1]
        genres = splitted_columns[2]
        yield "record", (title, genres)

    @staticmethod
    def transform_titles_and_genres_into_lists_mapper(_, input_row):
        yield "record", (input_row[0].split(' '), input_row[1].split('|'))

    # clean titles
    # title tags should not have numbers, prepositions, symbols
    @staticmethod
    def titles_normalization_mapper(_, input_row):
        keywords = input_row[0]
        genres = input_row[1]

        # filters out all non alphabetic elements, including numbers
        keywords = list(filter(str.isalpha, keywords))

        # filter conjunctions, prepositions or in other words - stop words
        keywords_filtered = []
        stop_words = set(stopwords.words('english'))
        for keyword in keywords:
            keyword = str.lower(keyword)
            if keyword not in stop_words:
                keywords_filtered.append(keyword)

        yield "record", (keywords_filtered, genres)

    @staticmethod
    def keyword_and_tags_mapper(_, input_row):
        keywords = input_row[0]
        genres = input_row[1]

        for keyword in keywords:
            for genre in genres:
                if genre != '(no genres listed)':
                    yield genre, (keyword, 1)

    # TODO: Need to be implemented correctly
    @staticmethod
    def genre_reducer(_, key, value):
        # pass
        yield key, (value[0], value[1] + 1)


if __name__ == '__main__':
    MRTopTenKeywordsForEachGenre.run()
