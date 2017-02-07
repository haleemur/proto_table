import pytest
import table

def test_init():
    d = {'A': [1, 1, 1, 3, 3], 'B': [1.2, 3.1, 34.1, 10, 3]}
    t = table.Table(d)
    assert list(t.generate_rows()) == [['A', 'B'],
                                       [1,1.2],
                                       [1,3.1], 
                                       [1,34.1], 
                                       [3,10], 
                                       [3,3]]


def test_raise_on_unequal_length():
    d = {'A': [1, 1, 1, 3, 3, 2, 3], 'B': [1.2, 3.1, 34.1, 10, 3]}
    with pytest.raises(AssertionError) as e:
        t = table.Table(d)
        assert e.value.message == 'Input Columns have different lengths'


def test_groupby_groups():
    d = {'category1': ['a', 'b', 'c', 'a', 'b', 'a', 'b'],
         'category2': ['x', 'y', 'x', 'y', 'x', 'y', 'x'],
         'category3': [  1,   1,   1,   2,   2,   2,   2]}
    t = table.Table(d)
    t = t.groupby(['category1', 'category2'])
    expected = sorted([(('a', 'x'), {0}), (('b', 'y'), {1}), (('c', 'x'), {2}),
                       (('a', 'y'), {3,5}), (('b', 'x'), {4,6}), (('c', 'y'), set())],
                      key=lambda x: x[0][0]+x[0][1])
    assert sorted(t.groups, key=lambda x: x[0][0]+x[0][1]) == expected


def test_groupby_aggregate():

    d = {'category1': ['a', 'b', 'c', 'a', 'b', 'a', 'b'],
         'category2': ['x', 'y', 'x', 'y', 'x', 'y', 'x'],
         'category3': [  1,   1,   1,   2,   2,   2,   2]}
    t = table.Table(d)
    t = t.groupby(['category1', 'category2'])
    t.agg({'category3': sum})
    expected = [['category1', 'category2', 'category3_agg0'],
                ['a', 'x', 1],
                ['a', 'y', 4],
                ['b', 'x', 4],
                ['b', 'y', 1],
                ['c', 'x', 1],
                ['c', 'y', 0]]
    assert list(t.generate_summary()) == expected
