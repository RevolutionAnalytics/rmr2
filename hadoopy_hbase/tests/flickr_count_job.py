#!/usr/bin/python
import hadoopy


def mapper(row, column_families):
    yield 'num_rows', 1

def reducer(key, values):
    yield key, sum(values)

if __name__ == '__main__':
    hadoopy.run(mapper, reducer)
