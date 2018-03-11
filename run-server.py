#!env/bin/python
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from recommender import config
from server import app

if __name__ == '__main__':
    debug_mode = len(sys.argv) > 1 and sys.argv[1] == 'debug'

    # Setup logging
    log_file = Path('.') / config.log_file
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True)
    log_level = logging.DEBUG  # logging.DEBUG if debug_mode else logging.INFO
    log_handler = RotatingFileHandler(str(log_file), maxBytes=1e7, backupCount=10)
    log_handler.setLevel(log_level)
    log_handler.setFormatter(logging.Formatter('[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'))
    app.logger.addHandler(log_handler)
    app.logger.setLevel(log_level)

    # Run the app
    app.run('127.0.0.1', 5000, debug=debug_mode)
