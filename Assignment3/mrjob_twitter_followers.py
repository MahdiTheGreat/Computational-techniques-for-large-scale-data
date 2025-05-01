 #!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.job import MRStep

class MRJobTwitterFollowers(MRJob):
    # The final (key,value) pairs returned by the class should be
    # 
    # yield ('most followers id', ???)
    # yield ('most followers', ???)
    # yield ('average followers', ???)
    # yield ('count no followers', ???)
    #
    # You will, of course, need to replace ??? with a suitable expression
    pass

    def mapper (self, _, line):
        user, follows = line.split(":")

        yield (user, 0)
        for user_id in follows.split():
            yield (user_id, 1)

    def combiner (self, user_id, counts):
        yield (user_id, sum(counts))

    def reducer (self, user_id, counts):
        yield (None, (user_id, sum(counts)))

    def reducer2 (self, _, users):
        max_user = [None, -1]
        num_users = 0
        num_zeroes = 0
        total_followers = 0

        for (user_id, followers) in users:
            num_users += 1
            if followers == 0:
                num_zeroes += 1
            else:
                if followers > max_user[1]:
                    max_user = [user_id, followers]
            total_followers += followers

        average = total_followers / num_users
        yield ('most followers id', max_user[0])
        yield ('most followers', max_user[1])
        yield ('average followers', average)
        yield ('count no followers', num_zeroes)

    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer), 
                MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    MRJobTwitterFollowers.run()

