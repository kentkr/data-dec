
from typing import Callable
import functools
from databricks.connect.session import DatabricksSession
from pyspark.sql import DataFrame
import functools

class Model:
    def __init__(self, function: Callable, path: str, *args, **kwargs) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.path = path
        self.args = args
        self.tests = []

    def __call__(self) -> DataFrame:
        print(f'Running {self.function.__name__}')
        return self.function(*self.args, **self.kwargs)

    def write(self) -> None:
        print(f'Writing {self.function.__name__} to {self.path}')
        df = self.function(*self.args)
        df.write.mode('overwrite').saveAsTable(self.path)

    def test(self) -> None:
        for test in self.tests:
            test()

    def show(self) -> None:
        print(f'Showing {self.function.__name__}')
        df = self.function(*self.args)
        df.show()


class Entity:
    models = {}
    def __init__(self) -> None:
        pass

    @classmethod
    def model(cls, path: str, *args, **kwargs):
        def decorator(fn: Callable) -> Callable:
            cls.models[fn.__name__] = Model(fn, path, *args, **kwargs)
            return fn
        return decorator

class Test:
    entity = Entity
    def __init__(self) -> None:
        pass

    @classmethod
    def not_empty(cls) -> Callable:
        def decorator(fn) -> Callable:
            # this wrapper allows function to not get called immediately
            def wrapper() -> None:
                spark = DatabricksSession.builder.profile('dev').getOrCreate()
                model = cls.entity.models[fn.__name__]
                df = spark.read.table(model.path)
                # test
                c = df.limit(1).collect()
                if len(c) > 0:
                    print('Test passes')
                else:
                    print('Test fails')
            cls.entity.models[fn.__name__].tests.append(wrapper)
            return wrapper
        return decorator

class Reference:
    entity = Entity

    @classmethod
    def file(cls, path: str) -> Callable:
        def decorator(fn: Callable) -> Callable:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs) -> Callable:
                return fn(*args, **kwargs)
            return wrapper
        return decorator

