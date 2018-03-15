#!env/bin/python
import sys

from recommender import config, utils
from server import app

if __name__ == '__main__':
    debug_mode = len(sys.argv) > 1 and sys.argv[1] == 'debug'

    # Setup logging
    utils.setup_logging(config.log_file, app.logger)

    # Run the app
    app.run('127.0.0.1', 5000, debug=debug_mode)
