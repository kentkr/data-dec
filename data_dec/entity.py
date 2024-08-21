
from typing import Callable
from pyspark.sql import DataFrame
from dataclasses import dataclass


@dataclass
class UnconfiguredTest:
    model: str
    name: str
    kwargs: dict

@dataclass
class Test:
    model: str
    name: str
    fn: Callable
    kwargs: dict

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

    def test(self) -> None:
        """Loop through tests for this function and test"""
        for test in self.tests:
            print(f'Testing model {self.name!r}, test {test.name}, args {test.kwargs}')
            print(test.fn(self, **test.kwargs))


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


