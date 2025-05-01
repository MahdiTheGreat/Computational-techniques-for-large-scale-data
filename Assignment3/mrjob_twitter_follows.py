 #!/usr/bin/env python3

from mrjob.job import MRJob

class MRJobTwitterFollows(MRJob):
    # The final (key,value) pairs returned by the class should be
    # 
    # yield ('most followed id', ???)
    # yield ('most followed', ???)
    # yield ('average followed', ???)
    # yield ('count follows no-one', ???)
    #
    # You will, of course, need to replace ??? with a suitable expression

    # yield None since calculations are done on one node in the reducer
    def mapper(self, _, line):
        user_id, follows = line.split(": ")
        if len(follows) > 0:
            yield (None, (user_id, len(follows.split(" "))))
        else:
            yield (None, (user_id, 0))

# No combiner needed, each line unique

    def reducer(self, _, users):
        max_user = [None, -1]
        num_users = 0
        num_zeroes = 0
        total_follows = 0

        for (user_id, follows) in users:
            num_users += 1
            if follows == 0:
                num_zeroes += 1
            else:
                if follows > max_user[1]:
                    max_user = [user_id, follows]
            total_follows += follows

        average = total_follows/num_users
        yield ('most followed id', max_user[0])
        yield ('most followed', max_user[1])
        yield ('average followed', average)
        yield ('count follows no-one', num_zeroes)


if __name__ == '__main__':
    MRJobTwitterFollows.run()

