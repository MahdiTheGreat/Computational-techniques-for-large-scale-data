from assignment3_problem1_skeleton import MRMineral
import time
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--num-workers', type=int, default=1)
    parser.add_argument('filename')
    args = parser.parse_args()
    mr_job = MRLettercount(args=['-r', 'local', '--num-cores',
                                 str(args.num_workers), '--ascii-only',
                                 args.filename])

    start = time.time()
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            print(key, value)
    end = time.time()
    print(f'Number of workers: {args.num_workers}')
    print(f'Time elapsed: {end-start} s')