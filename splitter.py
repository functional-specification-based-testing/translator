#!/usr/bin/env python3

from sys import argv, stderr
from itertools import groupby
from os import path
from operator import itemgetter


def main():
    if len(argv) != 3:
        print("usage:\t%s <input.mmtp> <output_dir>" % argv[0], file=stderr)
        exit(2)

    with open(argv[1]) as src:
        test_suite_size = int(next(src))
        test_cases = groupby(map(str.strip, src), lambda line: line != "")
        test_cases = filter(itemgetter(0), test_cases)
        test_cases = map(itemgetter(1), test_cases)
        test_cases = map("\n".join, test_cases)

        for idx, test_caes in enumerate(test_cases):
            with open(path.join(argv[2], "testcase%03d" % (idx+1)), "w") as dst:
                print(test_caes, file=dst)
    
    assert idx+1 == test_suite_size, "incomplete source file"


if __name__ == '__main__':
    main()
