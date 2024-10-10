from abc import ABC, abstractmethod
from .type_mappings import TypeMapper

class AbstractWarehouse(ABC):
    def __init__(self, warehouse_type):
        self.warehouse_type = warehouse_type

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    @abstractmethod
    def get_schema(self, full_table_name):
        pass

    @abstractmethod
    def create_table(self, schema_name, table_name, schema):
        pass

    @abstractmethod
    def get_data(self, table_name):
        pass

    @abstractmethod
    def insert_data(self, table_name, data):
        pass

    @abstractmethod
    def setup_target_environment(self):
        pass

    def map_type_to(self, target_warehouse_type, source_type):
        method_name = f"{self.warehouse_type}_to_{target_warehouse_type}"
        mapping_method = getattr(TypeMapper, method_name, None)
        if mapping_method:
            return mapping_method(source_type)
        else:
            raise NotImplementedError(f"Type mapping from {self.warehouse_type} to {target_warehouse_type} is not implemented")
