from abc import ABC, abstractmethod

class Backend(ABC):
    """Interface to implement for your backend
    """

    @abstractmethod
    def _execute_select():
        pass

    @abstractmethod
    def _execute_insert():
        pass

class JsonLBackend(Backend):
    """Operations for .jsonl based filesystem
    """
    def __init__(self):
        pass

    def _execute_select(self):
        pass

    def _execute_insert(self):
        pass

class CSVBackend(Backend):
    """Operations for .csv based filesystem
    """
    def __init__(self):
        pass

    def _execute_select(self):
        pass

    def _execute_insert(self):
        pass

class ParquetBackend(Backend):
    """Operations for .parquet based filesystem
    """
    def __init__(self):
        pass

    def _execute_select(self):
        pass

    def _execute_insert(self):
        pass