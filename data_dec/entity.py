
from collections import defaultdict
import functools
from typing import Callable
from dataclasses import dataclass
from pyspark.sql import DataFrame

class Model:
    def __init__(self, path: str, fn: Callable[[], DataFrame]) -> None:
        self.path = path
        self.fn = fn
        self.name = fn.__name__

    def write(self) -> None:
        print(f'Writing model {self.name!r} to table {self.path!r}')
        self.fn().write.mode('overwrite').saveAsTable(self.path)

    def test(self) -> None:
        entities = Entity
        print(f'Testing model {self.name}')
        for test in entities.tests[self.name]:
            print(f'Testing: {test.__name__!r}')
            print(test(self.fn))


class Test:
    @staticmethod
    def not_empty(fn: Callable[[], DataFrame]):
        df = fn()
        if len(df.collect()) > 0:
            return 'Test passes'
        else:
            return 'Test fails'

    @staticmethod
    def not_null(fn: Callable[[], DataFrame]):
        df = fn()
        rows = df.collect()
        # this is an example
        first_col = rows[0].__fields__[0]
        for row in rows:
            if not row[first_col]:
                return 'Test fails'
        return 'Test passes'


class Entity:
    models = {}
    tests = defaultdict(list)
    references = defaultdict(list)
    # no argument needed or possible
    @classmethod
    def register_model(cls, path: str):
        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)
            # Assign name if not provided
            model = Model(path=path, fn=fn)
            cls.models[fn.__name__] = model
            return wrapper
        return decorator

    # will only work with register_model
    @classmethod
    def register_test(cls, test_name):
        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)
            test_function = Test.__dict__[test_name]
            cls.tests[fn.__name__].append(test_function)
            return wrapper
        return decorator

    @classmethod
    def register_reference(cls, reference: str):
        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)
            cls.references[reference].append(fn.__name__)
            return wrapper
        return decorator



