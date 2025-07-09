"""Storage abstraction for local filesystem and S3."""
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Union

from ..logging import get_logger

logger = get_logger(__name__)


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def read(self, path: str) -> str:
        """Read file content as string."""
        pass
    
    @abstractmethod
    def write(self, path: str, content: str) -> None:
        """Write string content to file."""
        pass
    
    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if file exists."""
        pass
    
    @abstractmethod
    def list(self, prefix: str) -> List[str]:
        """List all files with given prefix."""
        pass
    
    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete a file."""
        pass


class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend."""
    
    def __init__(self, base_path: Union[str, Path]):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized local storage at {self.base_path}")
    
    def _full_path(self, path: str) -> Path:
        """Get full path by joining base path with given path."""
        return self.base_path / path
    
    def read(self, path: str) -> str:
        """Read file content as string."""
        full_path = self._full_path(path)
        logger.debug(f"Reading from {full_path}")
        try:
            return full_path.read_text(encoding='utf-8')
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")
        except Exception as e:
            logger.error(f"Error reading {path}: {e}")
            raise
    
    def write(self, path: str, content: str) -> None:
        """Write string content to file."""
        full_path = self._full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Writing to {full_path}")
        try:
            full_path.write_text(content, encoding='utf-8')
        except Exception as e:
            logger.error(f"Error writing {path}: {e}")
            raise
    
    def exists(self, path: str) -> bool:
        """Check if file exists."""
        return self._full_path(path).exists()
    
    def list(self, prefix: str) -> List[str]:
        """List all files with given prefix."""
        prefix_path = self._full_path(prefix)
        if prefix_path.is_dir():
            # List all files in directory
            files = []
            for item in prefix_path.rglob("*"):
                if item.is_file():
                    rel_path = item.relative_to(self.base_path)
                    files.append(str(rel_path))
            return sorted(files)
        else:
            # List files matching prefix
            parent = prefix_path.parent
            if not parent.exists():
                return []
            
            files = []
            prefix_name = prefix_path.name
            for item in parent.iterdir():
                if item.is_file() and item.name.startswith(prefix_name):
                    rel_path = item.relative_to(self.base_path)
                    files.append(str(rel_path))
            return sorted(files)
    
    def delete(self, path: str) -> None:
        """Delete a file."""
        full_path = self._full_path(path)
        if full_path.exists():
            logger.debug(f"Deleting {full_path}")
            full_path.unlink()


class StorageManager:
    """Main storage manager that handles different backends."""
    
    def __init__(self, storage_type: str = "local", **kwargs):
        """
        Initialize storage manager.
        
        Args:
            storage_type: Type of storage backend ("local" or "s3")
            **kwargs: Backend-specific arguments
                For local: base_path (default: "./data")
                For s3: bucket, prefix (to be implemented)
        """
        self.storage_type = storage_type
        
        if storage_type == "local":
            base_path = kwargs.get("base_path", "./data")
            self.backend = LocalStorageBackend(base_path)
        elif storage_type == "s3":
            raise NotImplementedError("S3 storage backend not yet implemented")
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")
        
        logger.info(f"Storage manager initialized with {storage_type} backend")
    
    def read(self, path: str) -> str:
        """Read file content as string."""
        return self.backend.read(path)
    
    def read_json(self, path: str) -> dict:
        """Read and parse JSON file."""
        content = self.read(path)
        return json.loads(content)
    
    def write(self, path: str, content: str) -> None:
        """Write string content to file."""
        self.backend.write(path, content)
    
    def write_json(self, path: str, data: dict, indent: int = 2) -> None:
        """Write data as JSON file."""
        content = json.dumps(data, indent=indent, default=str)
        self.write(path, content)
    
    def exists(self, path: str) -> bool:
        """Check if file exists."""
        return self.backend.exists(path)
    
    def list(self, prefix: str = "") -> List[str]:
        """List all files with given prefix."""
        return self.backend.list(prefix)
    
    def delete(self, path: str) -> None:
        """Delete a file."""
        self.backend.delete(path)