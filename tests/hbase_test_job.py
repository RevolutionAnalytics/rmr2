import hadoopy

def mapper(k, v):
    #yield 'KEY[%s]' % k, 'VALUE[%s]' % v
    yield k, v


if __name__ == '__main__':
    hadoopy.run(mapper)
