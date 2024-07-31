import sys

from .cli.bridge import main

DEFAULT_ROOT_DIR = "F:/"

try:
    main(sys.argv[1])
except IndexError:
    main(DEFAULT_ROOT_DIR)
