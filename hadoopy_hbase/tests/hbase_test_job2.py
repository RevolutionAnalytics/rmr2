import hadoopy


def mapper(row, column_families):
    for column_fam, columns in column_families.items():
        for column, data in columns.items():
            yield row, (column_fam, column, data)

if __name__ == '__main__':
    hadoopy.run(mapper)
