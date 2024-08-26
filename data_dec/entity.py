
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from dataclasses import dataclass
from data_dec.logging import logger
from termcolor import colored

@dataclass
class UnconfiguredTest:
    model_name: str
    name: str
    kwargs: dict

class Test:
    def __init__(self, model_name: str, name: str, fn, kwargs) -> None:
        self.model_name = model_name
        self.name = name
        self.fn = fn
        self.kwargs = kwargs

    def __call__(self, model) -> None:
        logger.info(msg=f'TESTING {self.model_name} with {self.name}')
        res = self.fn(model, **self.kwargs)
        if res:
            status = colored('PASS', 'green')
        else:
            status = colored('FAIL', 'red')
        logger.info(msg=f'TEST COMPLETE {self.model_name} with {self.name} result: {status}')

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
        logger.info(msg=f'RUNNING {self.name} to {path!r}')
        try:
            self.fn().write \
                .mode('overwrite') \
                .option('overwriteSchema', 'True') \
                .saveAsTable(path)
            logger.info(msg=f'{self.name} {colored('COMPLETE', 'green')}')
        except Exception as e:
            logger.error(msg=f'{self.name} {colored('ERROR', 'red')}\n{e}')


class TestFunctions:
    """Static functions that take a model and test it"""
    @staticmethod
    def not_empty(model: Model) -> bool:
        df = model.fn() # this calls the original function of the model
        count = df.limit(1).count()
        if count > 0:
            return True
        else:
            return False

    @staticmethod
    def not_null(model: Model, column: str) -> bool:
        df = model.fn()
        count = df.select(column).where(col(column).isNull()).count()
        if count == 0:
            return True
        else:
            return False


