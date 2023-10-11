
from mrjob.job import MRJob
from mrjob.job import MRStep


class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.my_mapper,
                   reducer=self.my_reducer)
        ]

    def my_mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def my_reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()

# execution
# python3 mr_wordcount.py word-count.txt