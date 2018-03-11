#!env/bin/python
import sys
from server import app

if __name__ == '__main__':
    debug_mode = len(sys.argv) > 1 and sys.argv[1] == 'debug'
    app.run('127.0.0.1', 5000, debug=debug_mode)
