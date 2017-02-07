## Prototype of a pivot table application in pure python

## Features:

### The following features have been successfully demonstrated, but are not robust yet:

* construct `Table` object from dictionary
* construct `Table` object from csv
* print the `Table` object
* create indexes on columns
* group by columns
* perform aggregate operations on a grouped `Table` object
* display raw `Table` data to console
* display summary table data from a grouped `Table` object

### The following features need implementing

* reshaping a grouped `Table` object, i.e. pivot table
* data ordering
* filtering

## Getting Started

This project requires python 3.3+ & pip to be available.

clone this repository to your local machine

```bash
git clone git@github.com:haleemur/proto_table.git
```

install required packages (Unix)

```bash
cd /path/to/proto_table && pip install -r requirements.txt
```

open a python shell and run the following example:

```python
from table import Table

# make some dummy data
d = {'category1': ['a', 'a', 'b', 'a', 'a', 'a', 'b'],
     'category2': ['x', 'y', 'x', 'y', 'x', 'y', 'x'],
     'category3': [  1,   1,   1,   2,   2,   2,   2]}

# create a Table from the dictionary
t = Table(d)

# outputs the data in a table
t.show()

# group & aggregate
t2 = t.groupby(['category1', 'category2']).agg({'category3': sum})


# outputs the summary in a table.
t2.show()
```

A more complex pivot table illustrates aggregation on multiple columns

```python
from table import Table

d = {'user_id': [1, 1, 1, 2, 2, 2, 3, 3, 3],
     'store_id': ['a', 'a', 'a', 'a', 'b', 'b', 'c', 'a', 'a'],
     'revenue': [10, 20, 30, 10, 10, 20, 15, 20, 25]}

t = Table(d)

t2 = t.groupby(['store_id']).agg({'user_id': len, 'revenue': sum})

t2.show()
