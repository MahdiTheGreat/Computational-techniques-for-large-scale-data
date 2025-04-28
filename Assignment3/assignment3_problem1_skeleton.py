#!/usr/bin/env python3

from mrjob.job import MRJob

class MRMineral(MRJob):
    def mapper(self, _, line):
        line = line.split(",")
        if line[0] == "Constellation": #This is the header
            return
        if line[1] == "Prime":
            yield(line[0], int(line[6])) #(StarConstellation, RU)
        else:
            yield(line[1] + " " + line[0], int(line[6])) #(StarConstellation, RU)

    def combiner(self, star, RUs):
        yield(star, sum(RUs))

    def reducer(self, star, RUs):
        yield(star, sum(RUs))

if __name__ == '__main__':
    MRMineral().run()