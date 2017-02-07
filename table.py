import csv
from collections import defaultdict
from itertools import product
import sys


class Table(object):
    
    def __init__(self, data, datetime_columns=None):
        self.indexes = {} 
        self.data = {}
        self.groupby_columns = None
        lengths = set(len(x) for x in data.values())
        assert len(lengths) == 1, 'Input Columns have different lengths'
        self.length = list(lengths)[0]
        for k, v in data.items():
            self.data[k] = v

        if datetime_columns is not None:
            for col in datetime_columns:
                self.data[k] = [strptime(i,'%Y-%m-%d') for i in self.data[k]]

    def read_csv(self, path, header=True, delimter=',', datetime_columns=None):
        with open(path) as f:
            rd = csv.reader(f, delimiter=delimiter)
            if header is True:
                columns = next(rd) 
            row_data = [row for row in rd]
        t = Table(dict(zip(columns, zip(*row_data)), datetime_columns))
        return t 

    def show(self, rownumbers=False):
        if self.groupby_columns is not None:
            for row in self.generate_summary(rownumbers):
                print('  '.join([str(cell).rjust(self.widths[i]) for i, cell in enumerate(row)]))
        else:
            for row in self.generate_rows(rownumbers):
                print('  '.join([str(cell).rjust(self.widths[i]) for i, cell in enumerate(row)]))

    def generate_rows(self, rownumbers=False): 
        columns = list(self.data.keys())

        widths = [max(len(k), *(len(str(i)) for i in v)) for k, v in self.data.items()]

        if rownumbers is True:
            self.widths = [self.length] + widths
            yield ['*'] + columns
        else:
            self.widths = widths
            yield columns

        for i in range(0, self.length):
            row = []
            if rownumbers is True:
                row.append(i)
            for c in columns:
                row.append(self.data[c][i])
            yield row
   
    def build_indexes(self, columns):
        for c in columns:
            if c not in self.indexes.keys():
                self.indexes[c] = defaultdict(list)
                for i, v in enumerate(self.data[c]):
                    self.indexes[c][v].append(i)
 
    def groupby(self, columns):
        self.build_indexes(columns) 
        self.groupby_columns = columns
        self.product = product(*[list(self.indexes[column].keys()) for column in columns])

        self.groups = [(group, set.intersection(*[set(self.indexes[column][level])
                                                 for column, level in zip(columns, group)]))
                       for group in self.product]
        return self

    def agg(self, mappings, ignore_nulls=True):
        self._aggregates = {}
        try:
            indexes = [index for group, index in self.groups]
        except AttributeError:
            indexes = [range(self.length)]    

        i = 0
        for column, functions in mappings.items():
            if not isinstance(functions, (list, tuple)):
                functions = [functions]
            for fn in functions:
                agg_name = column + '_agg' + str(i)
                agg_splits = ([i for j, i in enumerate(self.data[column]) if j in index]
                              for index in indexes)
                self._aggregates[agg_name] = [fn(split) for split in agg_splits]
                i += 1
        return self

    def generate_summary(self, rownumbers=False):
        columns = list(self._aggregates.keys())
        index_widths = [max(len(column), *(len(str(x)) for x in self.data[column])) 
                        for column in self.groupby_columns]
        widths = [max(len(k), *(len(str(i)) for i in v)) 
                  for k, v in self._aggregates.items()]

        header = self.groupby_columns + columns

        if rownumbers is True:
            self.widths = [len(self.groups)] + index_widths + widths
            yield ['*'] + header
        else:
            self.widths = index_widths + widths
            yield header
        for i in range(len(self.groups)):
            row = []
            if rownumbers is True:
                row.append(i)
            row += [j for j in self.groups[i][0]]
            for c in columns:
                row.append(self._aggregates[c][i])
            yield row
