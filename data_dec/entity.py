
from collections import defaultdict
import functools
from typing import Callable, List
from pyspark.sql import DataFrame
from dataclasses import dataclass

class Model:
    """Model class. Stores model metadata and can write/test a model"""
    def __init__(self, path: str, fn: Callable[[], DataFrame]) -> None:
        self.path = path
        self.fn = fn
        self.name = fn.__name__

    def write(self) -> None:
        """Save model as spark table"""
        print(f'Writing model {self.name!r} to table {self.path!r}')
        self.fn().write.mode('overwrite').saveAsTable(self.path)

    def test(self) -> None:
        """Loop through tests for this function and test"""
        entities = Entity
        for test in entities.tests[self.name]:
            print(f'Testing model {self.name!r} for test {test.fn.__name__!r} using args {test.kwargs}')
            print(test.fn(self, **test.kwargs))

@dataclass
class Test:
    fn: Callable
    kwargs: dict

class TestFunctions:
    """Static functions that take a model and test it"""
    @staticmethod
    def not_empty(model: Model):
        df = model.fn() # this calls the original function of the model
        if len(df.collect()) > 0:
            return 'Test passes'
        else:
            return 'Test fails'

    @staticmethod
    def not_null(model: Model, column: str):
        df = model.fn()
        rows = df.collect()
        # this is an example
        for row in rows:
            if not row[column]:
                return f'Test fails'
        return 'Test passes'


class Entity:
    """
    Entities class to keeps track of all models, tests, and references.

    The methods are used as decorators.
    """
    models: dict[str, Model] = {}
    tests: dict[str, List[Test]] = defaultdict(list)
    references: dict[str, List[str]] = defaultdict(list)

    @classmethod
    def register_model(cls, path: str) -> Callable:
        """
        Register a model. Model names are the name of the function. Path is where it will get
        written to
        """
        def decorator(fn: Callable[[], DataFrame]) -> Callable:
            # this keeps __name__ and __doc__ related to the wrapped function
            @functools.wraps(fn)
            # this always returns the decorated function, no modifications
            def wrapper(*args, **kwargs) -> DataFrame:
                return fn(*args, **kwargs)
            # Create a Model and assign it to models
            model = Model(path=path, fn=fn)
            cls.models[fn.__name__] = model
            return wrapper
        return decorator

    # will only work with register_model
    @classmethod
    def register_test(cls, test_name: str, **kwargs) -> Callable:
        """
        Register a test. Models use tests later when testing.
        """
        def decorator(fn: Callable) -> Callable:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs) -> DataFrame:
                return fn(*args, **kwargs)
            test = Test(TestFunctions.__dict__[test_name], kwargs)
            cls.tests[fn.__name__].append(test)
            return wrapper
        return decorator

    @classmethod
    def register_reference(cls, reference: str) -> Callable:
        """
        Register a reference: a model that the decorated function depends on.
        """
        def decorator(fn: Callable) -> Callable:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs) -> DataFrame:
                return fn(*args, **kwargs)
            cls.references[reference].append(fn.__name__)
            return wrapper
        return decorator


