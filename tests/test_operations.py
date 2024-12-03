from tinydb import where
from tinydb.operations import delete, increment, decrement, add, subtract, set


from tinydb import where

def print_db_contents(db):
    print("Database contents:", db.all())

def test_delete(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(delete('int'), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'a')
    print("Delete result:", result)
    assert result is not None and 'int' not in result


def test_add_int(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(add('int', 5), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'a')
    print("Add int result:", result)
    assert result is not None and result['int'] == 6


def test_add_str(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(add('char', 'xyz'), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'axyz')
    print("Add str result:", result)
    assert result is not None and result['int'] == 1


def test_subtract(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(subtract('int', 5), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'a')
    print("Subtract result:", result)
    assert result is not None and result['int'] == -4


def test_set(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(set('char', 'xyz'), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'xyz')
    print("Set result:", result)
    assert result is not None and result['int'] == 1


def test_increment(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(increment('int'), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'a')
    print("Increment result:", result)
    assert result is not None and result['int'] == 2


def test_decrement(db):
    db.insert({'char': 'a', 'int': 1})
    print_db_contents(db)
    db.update(decrement('int'), where('char') == 'a')
    print_db_contents(db)
    result = db.get(where('char') == 'a')
    print("Decrement result:", result)
    assert result is not None and result['int'] == 0
