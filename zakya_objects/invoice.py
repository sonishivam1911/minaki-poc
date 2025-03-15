from typing import Any, Dict
from zakya_objects.data_provider_main import ZakyaDataProvider

class Invoice(ZakyaDataProvider):

    def create(self, data: Dict[str, Any]):
        return super().create(data)
    
    def update(self, record_id: str, data: Dict[str, Any]):
        return super().update(record_id, data)
    
    def read(self, record_id: Any):
        return super().read(record_id)
    
    def read_all(self, filters: Dict[str, Any] = None):
        return super().read_all(filters)