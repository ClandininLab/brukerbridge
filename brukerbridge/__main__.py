import sys

from .cli.bridge import main

DEFAULT_ROOT_DIR = "D:/"

try:
    main(sys.argv[1])
except IndexError:
    main(DEFAULT_ROOT_DIR)
