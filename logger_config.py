#!/usr/bin/env python
"""
Logger Configuration Module
--------------------------
This module configures the logging system with emoji-based formatting.
"""

import logging

class EmojiLogFormatter(logging.Formatter):
    """Custom log formatter that uses emojis for log levels."""
    
    FORMATS = {
        logging.DEBUG: "🔍 %(message)s",
        logging.INFO: "💬 %(message)s",
        logging.WARNING: "⚠️ %(message)s",
        logging.ERROR: "❌ %(message)s",
        logging.CRITICAL: "🔥 %(message)s"
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logging(level=logging.INFO):
    """
    Set up logging with emoji formatter.
    
    Args:
        level: The logging level to use
    """
    handler = logging.StreamHandler()
    handler.setFormatter(EmojiLogFormatter())
    logging.basicConfig(
        level=level,
        handlers=[handler]
    )
    return logging.getLogger(__name__)
