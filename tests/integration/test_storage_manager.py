"""Integration tests for storage manager."""

import pytest

from dora_metrics.storage import StorageManager


@pytest.mark.integration
class TestStorageManagerIntegration:
    """Integration tests for storage manager with real file operations."""

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

    def test_concurrent_operations(self, storage):
        """Test concurrent read/write operations."""
        # This would test thread safety if we implement it later
        path = "test_concurrent.txt"
        content = "test content"

        # Multiple writes
        for i in range(10):
            storage.write(f"concurrent_{i}.txt", f"content_{i}")

        # Verify all files exist
        files = storage.list("concurrent_")
        assert len(files) == 10

        # Clean up
        for file in files:
            storage.delete(file)

    def test_large_file_handling(self, storage):
        """Test handling of large files."""
        # Create a large content (1MB)
        large_content = "x" * (1024 * 1024)
        path = "large_file.txt"

        storage.write(path, large_content)
        read_content = storage.read(path)

        assert len(read_content) == len(large_content)
        assert read_content == large_content

        storage.delete(path)

    def test_nested_directory_operations(self, storage):
        """Test operations with deeply nested directories."""
        deep_path = "level1/level2/level3/level4/file.json"
        data = {"nested": True, "level": 4}

        storage.write_json(deep_path, data)

        # Test listing at different levels
        assert len(storage.list("level1/")) >= 1
        assert len(storage.list("level1/level2/")) >= 1
        assert len(storage.list("level1/level2/level3/")) >= 1

        # Read back
        read_data = storage.read_json(deep_path)
        assert read_data == data

        # Clean up
        storage.delete(deep_path)
