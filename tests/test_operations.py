from tinydb import where
from tinydb.operations import delete, increment, decrement, add, subtract, set


def test_delete(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(delete('int'), where('char') == 'a')
    result = db.get(where('char') == 'a')
    assert result is not None and 'int' not in result


def test_add_int(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(add('int', 5), where('char') == 'a')
    result = db.get(where('char') == 'a')
    assert result is not None and result['int'] == 6


def test_add_str(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(add('char', 'xyz'), where('char') == 'a')
    result = db.get(where('char') == 'axyz')
    assert result is not None and result['int'] == 1


def test_subtract(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(subtract('int', 5), where('char') == 'a')
    result = db.get(where('char') == 'a')
    assert result is not None and result['int'] == -4


def test_set(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(set('char', 'xyz'), where('char') == 'a')
    result = db.get(where('char') == 'xyz')
    assert result is not None and result['int'] == 1


def test_increment(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(increment('int'), where('char') == 'a')
    result = db.get(where('char') == 'a')
    assert result is not None and result['int'] == 2


def test_decrement(db):
    db.insert({'char': 'a', 'int': 1})
    db.update(decrement('int'), where('char') == 'a')
    result = db.get(where('char') == 'a')
    assert result is not None and result['int'] == 0
