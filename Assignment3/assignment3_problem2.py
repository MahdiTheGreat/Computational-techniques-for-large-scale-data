#!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.job import MRStep

class MRMineral(MRJob):
    def configure_args(self):
        super(MRMineral, self).configure_args()
        self.add_passthru_arg("-k", "--k-most-valuable", type=int, default=0, 
            help="List the k most valuable starsystems, outputs all star systems in arbitrary order if 0 or empty.")

    def mapper(self, _, line):
        line = line.split(",")
        if line[0] == "Constellation": #This is the header, ignore
            pass
        else:
            yield(line[0] if line[1] == "Prime" else line[1] + " " + line[0], int(line[5])) 

    def combiner(self, star, RUs):
        yield(star, sum(RUs))

    def reducer(self, star, RUs):
        yield(None, (star, sum(RUs)))

    # Yields the k star systems with the largest RU values in descending order
    # or all star systems in arbitrary order if k == 0
    def reducer2(self, _, stars):
        k = self.options.k_most_valuable
        if k > 0:
            top_k = []
            for star, total_RU in stars:
                if len(top_k) < k:
                    # Append in this order to make RUs the value compared on min() function
                    top_k.append((total_RU, star)) 
                else:
                    min_RU = min(top_k)
                    if (total_RU, star) > min(top_k):
                        top_k.remove(min_RU)
                        top_k.append((total_RU, star))
                        
            top_k.sort(reverse=True)
            for i in range(k):
                yield(top_k[i][1], top_k[i][0]) # Yield in order starsystem, RU
                    
        else:
            for (star, total_RU) in stars:
                yield(star, total_RU)

    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer), 
                MRStep(reducer=self.reducer2)]
    
if __name__ == '__main__':
    MRMineral().run()