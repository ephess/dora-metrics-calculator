"""Unit tests for storage manager."""
import json
import pytest
from pathlib import Path
import tempfile
import shutil

from dora_metrics.storage import StorageManager


class TestLocalStorageBackend:
    """Test local filesystem storage backend."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def storage(self, temp_dir):
        """Create a storage manager instance."""
        return StorageManager(storage_type="local", base_path=temp_dir)
    
    def test_init_creates_base_directory(self, temp_dir):
        """Test that initialization creates the base directory."""
        base_path = Path(temp_dir) / "test_data"
        assert not base_path.exists()
        
        StorageManager(storage_type="local", base_path=str(base_path))
        assert base_path.exists()
    
    def test_write_and_read(self, storage):
        """Test writing and reading a file."""
        content = "Hello, World!"
        path = "test.txt"
        
        storage.write(path, content)
        assert storage.read(path) == content
    
    def test_write_creates_subdirectories(self, storage):
        """Test that write creates necessary subdirectories."""
        content = "Test content"
        path = "subdir/nested/file.txt"
        
        storage.write(path, content)
        assert storage.read(path) == content
    
    def test_read_nonexistent_file(self, storage):
        """Test reading a non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="File not found: nonexistent.txt"):
            storage.read("nonexistent.txt")
    
    def test_exists(self, storage):
        """Test checking file existence."""
        path = "test.txt"
        
        assert not storage.exists(path)
        storage.write(path, "content")
        assert storage.exists(path)
    
    def test_delete(self, storage):
        """Test deleting a file."""
        path = "test.txt"
        storage.write(path, "content")
        
        assert storage.exists(path)
        storage.delete(path)
        assert not storage.exists(path)
    
    def test_delete_nonexistent_file(self, storage):
        """Test deleting non-existent file doesn't raise error."""
        storage.delete("nonexistent.txt")  # Should not raise
    
    def test_list_files_in_directory(self, storage):
        """Test listing files in a directory."""
        # Create some test files
        storage.write("dir1/file1.txt", "content1")
        storage.write("dir1/file2.txt", "content2")
        storage.write("dir2/file3.txt", "content3")
        
        # List files in dir1
        files = storage.list("dir1")
        assert sorted(files) == ["dir1/file1.txt", "dir1/file2.txt"]
    
    def test_list_files_with_prefix(self, storage):
        """Test listing files with a prefix."""
        # Create test files
        storage.write("test_1.json", "{}")
        storage.write("test_2.json", "{}")
        storage.write("other.json", "{}")
        
        # List files starting with "test_"
        files = storage.list("test_")
        assert sorted(files) == ["test_1.json", "test_2.json"]
    
    def test_list_empty_directory(self, storage):
        """Test listing an empty or non-existent directory."""
        files = storage.list("empty_dir")
        assert files == []
    
    def test_json_operations(self, storage):
        """Test JSON read/write operations."""
        data = {
            "key": "value",
            "number": 42,
            "list": [1, 2, 3],
            "nested": {"a": 1, "b": 2}
        }
        path = "test.json"
        
        storage.write_json(path, data)
        loaded_data = storage.read_json(path)
        
        assert loaded_data == data
    
    def test_json_with_datetime(self, storage):
        """Test JSON serialization with datetime objects."""
        from datetime import datetime
        
        data = {
            "timestamp": datetime(2024, 1, 1, 12, 0, 0),
            "name": "test"
        }
        path = "test_datetime.json"
        
        storage.write_json(path, data)
        content = storage.read(path)
        
        # Check that datetime was serialized as string
        assert "2024-01-01" in content
    
    def test_unicode_content(self, storage):
        """Test handling Unicode content."""
        content = "Hello 世界 🌍"
        path = "unicode.txt"
        
        storage.write(path, content)
        assert storage.read(path) == content
    
    def test_storage_type_validation(self):
        """Test that invalid storage type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown storage type: invalid"):
            StorageManager(storage_type="invalid")
    
    def test_s3_not_implemented(self):
        """Test that S3 storage raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="S3 storage backend not yet implemented"):
            StorageManager(storage_type="s3")


class TestStorageManagerIntegration:
    """Integration tests for storage manager."""
    
    @pytest.fixture
    def storage(self, tmp_path):
        """Create a storage manager with tmp_path."""
        return StorageManager(storage_type="local", base_path=str(tmp_path))
    
    def test_complex_workflow(self, storage):
        """Test a complex workflow with multiple operations."""
        # Write multiple files
        storage.write("raw/commits.json", '{"commits": []}')
        storage.write("raw/prs.json", '{"prs": []}')
        storage.write("processed/data.csv", "id,name\n1,test")
        
        # List all files
        all_files = storage.list("")
        assert len(all_files) == 3
        
        # List by prefix
        raw_files = storage.list("raw/")
        assert len(raw_files) == 2
        
        # Read and modify JSON
        commits_data = storage.read_json("raw/commits.json")
        commits_data["commits"].append({"sha": "abc123"})
        storage.write_json("raw/commits.json", commits_data)
        
        # Verify modification
        updated_data = storage.read_json("raw/commits.json")
        assert len(updated_data["commits"]) == 1
        assert updated_data["commits"][0]["sha"] == "abc123"
        
        # Clean up
        for file in all_files:
            storage.delete(file)
        
        assert storage.list("") == []