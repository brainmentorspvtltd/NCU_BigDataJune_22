from mrjob.job import MRJob

class WordCount(MRJob):

    def mapper(self, k, line):
        words = line.split()
        for word in words:
            yield(word, 1)

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    WordCount.run()


# find out how many words starts with 'h'
# find out word with max count
# find out word with max length