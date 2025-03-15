from abc import ABC, abstractmethod
from typing import Dict, Any

class ZakyaDataProvider(ABC):
    """Abstract base class for all Zakya operations"""
    
    @abstractmethod
    def create(self, data: Dict[str, Any]):
        """Create a new record"""
        pass
    
    @abstractmethod
    def update(self, record_id: str, data: Dict[str, Any]):
        """Update an existing record"""
        pass
    
    @abstractmethod
    def read(self, record_id: Any):
        """Read a single record by ID"""
        pass
    
    @abstractmethod
    def read_all(self, filters: Dict[str, Any] = None):
        """Read all records with optional filtering"""
        pass
