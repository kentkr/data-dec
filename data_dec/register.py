
from collections import defaultdict
import functools
from typing import Callable, List
from pyspark.sql import DataFrame
from data_dec.entity import UnconfiguredTest

class Register:
    """
    Entities class to keeps track of all models, tests, and references.

    The methods are used as decorators.
    """
    models: List[Callable] = []
    tests: dict[str, List[UnconfiguredTest]] = defaultdict(list)
    references: dict[str, List[str]] = defaultdict(list)

    @classmethod
    def model(cls) -> Callable:
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
            cls.models.append(fn)
            return wrapper
        return decorator

    # will only work with register_model
    @classmethod
    def model_test(cls, test_name: str, **kwargs) -> Callable:
        """
        Register a test. Models use tests later when testing.
        """
        def decorator(fn: Callable) -> Callable:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs) -> DataFrame:
                return fn(*args, **kwargs)
            test = UnconfiguredTest(model_name = fn.__name__, name = test_name, kwargs = kwargs)
            cls.tests[fn.__name__].append(test)
            return wrapper
        return decorator

    @classmethod
    def reference(cls, reference: str) -> Callable:
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

