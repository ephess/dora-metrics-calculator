"""Unit tests for logging configuration."""
import logging
import tempfile
from pathlib import Path

import pytest

from dora_metrics.logging import setup_logging, get_logger


class TestLogging:
    """Test logging configuration."""
    
    @pytest.fixture(autouse=True)
    def reset_logging(self):
        """Reset logging configuration after each test."""
        yield
        # Clear all handlers
        logger = logging.getLogger()
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
    
    def test_setup_logging_default(self, caplog):
        """Test default logging setup."""
        setup_logging()
        logger = get_logger("test")
        
        with caplog.at_level(logging.INFO):
            logger.info("Test info message")
            logger.debug("Test debug message")  # Should not appear with INFO level
        
        assert "Test info message" in caplog.text
        assert "Test debug message" not in caplog.text
    
    def test_setup_logging_debug_level(self, caplog):
        """Test logging with DEBUG level."""
        setup_logging(level="DEBUG")
        logger = get_logger("test")
        
        with caplog.at_level(logging.DEBUG):
            logger.debug("Test debug message")
        
        assert "Test debug message" in caplog.text
    
    def test_setup_logging_with_file(self):
        """Test logging to file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            log_file = f.name
        
        try:
            # Clear any existing handlers first
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            setup_logging(log_file=log_file)
            logger = get_logger("test")
            
            logger.info("Test file message")
            
            # Force flush all handlers
            logging.shutdown()
            
            # Re-initialize handlers after shutdown
            setup_logging(log_file=log_file)
            
            # Check file content
            content = Path(log_file).read_text()
            assert "Test file message" in content
        finally:
            if Path(log_file).exists():
                Path(log_file).unlink()
    
    def test_third_party_loggers_suppressed(self, capsys):
        """Test that third-party loggers are set to WARNING."""
        setup_logging(level="DEBUG")
        
        # These loggers should be suppressed
        urllib_logger = logging.getLogger("urllib3")
        git_logger = logging.getLogger("git")
        boto_logger = logging.getLogger("boto3")
        
        assert urllib_logger.getEffectiveLevel() == logging.WARNING
        assert git_logger.getEffectiveLevel() == logging.WARNING
        assert boto_logger.getEffectiveLevel() == logging.WARNING
    
    def test_get_logger_returns_named_logger(self):
        """Test that get_logger returns correctly named logger."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        
        assert logger1.name == "module1"
        assert logger2.name == "module2"
        assert logger1 is not logger2
    
    def test_log_format(self, caplog):
        """Test that log format includes all expected fields."""
        setup_logging()
        logger = get_logger("test.module")
        
        with caplog.at_level(logging.INFO):
            logger.info("Test message")
        
        # Should contain: timestamp - logger name - level - message
        assert "test.module" in caplog.text
        assert "INFO" in caplog.text
        assert "Test message" in caplog.text