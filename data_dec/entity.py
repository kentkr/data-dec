
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from dataclasses import dataclass


@dataclass
class UnconfiguredTest:
    model: str
    name: str
    kwargs: dict

class Test:
    def __init__(self, model: str, name: str, fn, kwargs) -> None:
        self.model = model
        self.name = name
        self.fn = fn
        self.kwargs = kwargs

    def __call__(self, model) -> None:
        print(f'Testing {self.model}, test {self.name}, kwargs {self.kwargs}')
        print(self.fn(model, **self.kwargs))

class Model:
    """Model class. Stores model metadata and can write/test a model"""
    def __init__(
            self, 
            fn: Callable[[], DataFrame], 
            database: str, 
            schema: str, 
            tests: list[Test] = None
        ) -> None:
        self.fn = fn
        self.name = fn.__name__
        self.database = database
        self.schema = schema
        # can't assign test = [] bc tests will be shared across classes
        if tests:
            self.tests = tests
        else:
            self.tests = []

    def write(self) -> None:
        """Save model as spark table"""
        path = '.'.join([self.database, self.schema, self.name])
        print(f'Writing model {self.name!r} to table {path!r}')
        self.fn().write \
            .mode('overwrite') \
            .option('overwriteSchema', 'True') \
            .saveAsTable(path)


class TestFunctions:
    """Static functions that take a model and test it"""
    @staticmethod
    def not_empty(model: Model):
        df = model.fn() # this calls the original function of the model
        count = df.limit(1).count()
        if count > 0:
            return 'Test passes'
        else:
            return 'Test fails'

    @staticmethod
    def not_null(model: Model, column: str):
        df = model.fn()
        count = df.select(column).where(col(column).isNull()).count()
        if count == 0:
            return 'Test passes'
        else:
            return f'Test fails'


